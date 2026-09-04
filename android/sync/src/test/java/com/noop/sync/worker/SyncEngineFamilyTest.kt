package com.noop.sync.worker

import com.noop.sync.DailyMetricObservation
import com.noop.sync.HrObservation
import com.noop.sync.ObservationSource
import com.noop.sync.RrIntervalObservation
import com.noop.sync.SleepSessionObservation
import com.noop.sync.SyncContract
import com.noop.sync.SyncDiagnostics
import com.noop.sync.SyncRepository
import com.noop.sync.SyncWatermark
import com.noop.sync.api.SyncApiClient
import com.noop.sync.api.SyncCallResult
import com.noop.sync.api.SyncConfig
import com.noop.sync.capture.FileSegmentStore
import com.noop.sync.capture.RawJournalWriter
import com.noop.sync.dto.DailyObservationsRequestDto
import com.noop.sync.dto.IngestAckDto
import com.noop.sync.dto.RrIntervalsRequestDto
import com.noop.sync.dto.SleepSessionsRequestDto
import com.noop.sync.pairing.DeviceCredentials
import com.noop.sync.pairing.DeviceTokenStore
import com.noop.sync.queue.FileSyncQueue
import com.noop.sync.queue.QueueEntryState
import java.io.File
import java.util.concurrent.Executor
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

/*
 * Engine-level family drain tests (fork addition): the families drain in the frozen order, their
 * watermarks advance ONLY on accepted acks (or an explicit skip), a rejected family parks its
 * window without halting the others, and an R-R backlog chunks at the 20k batch cap.
 *
 * Everything is plain JVM: a fake ObservationSource, a stubbed SyncApiClient (the class is open
 * exactly for this), a synchronous journal writer, and a temp-dir queue/watermark.
 */
class SyncEngineFamilyTest {

    @get:Rule
    val tmp = TemporaryFolder()

    private val tokenStore = MemTokenStore("device-token")

    private fun ack(batchId: String, accepted: Boolean = true) = IngestAckDto(
        batchId = batchId, accepted = accepted, recordsReceived = 0, recordsInserted = 0,
        recordsDuplicate = 0, rawAck = false, rawFrameCount = 0, rawBytesStored = 0,
        warnings = emptyList(), serverTime = "2026-09-04T06:41:05Z",
    )

    private class StubApi : SyncApiClient(SyncConfig(decoderVersion = "noop-android/test+somatriq")) {
        val order = mutableListOf<String>()
        val dailyReqs = mutableListOf<DailyObservationsRequestDto>()
        val sleepReqs = mutableListOf<SleepSessionsRequestDto>()
        val rrReqs = mutableListOf<RrIntervalsRequestDto>()
        var dailyResult: (String) -> SyncCallResult<IngestAckDto> = { SyncCallResult.Ok(okAck()) }
        var sleepResult: (String) -> SyncCallResult<IngestAckDto> = { SyncCallResult.Ok(okAck()) }
        var rrResult: (String) -> SyncCallResult<IngestAckDto> = { SyncCallResult.Ok(okAck()) }

        override fun postDailyObservations(deviceToken: String, request: DailyObservationsRequestDto): SyncCallResult<IngestAckDto> {
            dailyReqs.add(request)
            order.add("daily")
            return dailyResult(request.batchId)
        }

        override fun postSleepSessions(deviceToken: String, request: SleepSessionsRequestDto): SyncCallResult<IngestAckDto> {
            sleepReqs.add(request)
            order.add("sleep")
            return sleepResult(request.batchId)
        }

        override fun postRrIntervals(deviceToken: String, request: RrIntervalsRequestDto): SyncCallResult<IngestAckDto> {
            rrReqs.add(request)
            order.add("rr")
            return rrResult(request.batchId)
        }

        companion object {
            fun okAck() = IngestAckDto(
                batchId = "stub", accepted = true, recordsReceived = 0, recordsInserted = 0,
                recordsDuplicate = 0, rawAck = false, rawFrameCount = 0, rawBytesStored = 0,
                warnings = emptyList(), serverTime = "2026-09-04T06:41:05Z",
            )
        }
    }

    private class MemTokenStore(private val t: String?) : DeviceTokenStore {
        override fun save(credentials: DeviceCredentials) = Unit
        override fun load(): DeviceCredentials? = null
        override fun token(): String? = t
        override fun clear() = Unit
    }

    /** In-memory source; every family pages forward from the given key over its list. */
    private class FakeSource(
        val daily: List<DailyMetricObservation> = emptyList(),
        val sleep: List<SleepSessionObservation> = emptyList(),
        val rr: List<RrIntervalObservation> = emptyList(),
        val hr: List<HrObservation> = emptyList(),
    ) : ObservationSource {
        override suspend fun hrSamples(fromTsExclusive: Long, toTsInclusive: Long, limit: Int): List<HrObservation> =
            hr.filter { it.tsEpochSeconds in (fromTsExclusive + 1)..toTsInclusive }.take(limit)

        override suspend fun maxHrTs(): Long? = hr.maxOfOrNull { it.tsEpochSeconds }
        override suspend fun todayDay(): String? = null // today-gating is the app adapter's job
        override suspend fun dailyMetrics(fromDayInclusive: String, limit: Int): List<DailyMetricObservation> =
            daily.filter { it.day >= fromDayInclusive }.take(limit)

        override suspend fun sleepSessions(fromStartTsInclusive: Long, limit: Int): List<SleepSessionObservation> =
            sleep.filter { it.startTsEpochSeconds >= fromStartTsInclusive }.take(limit)

        override suspend fun rrIntervals(fromTsInclusive: Long, limit: Int): List<RrIntervalObservation> =
            rr.filter { it.tsEpochSeconds >= fromTsInclusive }.take(limit)
    }

    /** Mutable clock so a test can step past the retry backoff. */
    private class TestHarn(
        val api: StubApi,
        val wm: SyncWatermark,
        val queue: FileSyncQueue,
        val engine: SyncEngine,
        private val now: java.util.concurrent.atomic.AtomicLong,
    ) {
        var nowMs: Long
            get() = now.get()
            set(value) = now.set(value)

        fun pass(): EnginePassResult = runBlocking { engine.runPass() }
    }

    private fun harness(source: FakeSource, api: StubApi): TestHarn {
        val now = java.util.concurrent.atomic.AtomicLong(1_000)
        val journal = RawJournalWriter(FileSegmentStore(File(tmp.newFolder(), "raw")), Executor { it.run() })
        val wm = SyncWatermark(File(tmp.newFolder(), "wm.json"))
        val queue = FileSyncQueue(File(tmp.newFolder(), "queue"))
        val engine = SyncEngine(
            queue = queue,
            journal = journal,
            repository = SyncRepository(source),
            watermark = wm,
            apiClient = api,
            tokenStore = tokenStore,
            diagnostics = SyncDiagnostics(),
            clock = { now.get() },
        )
        return TestHarn(api, wm, queue, engine, now)
    }

    @Test
    fun `families drain in order and advance their watermarks on accepted acks`() {
        val api = StubApi()
        val h = harness(
            FakeSource(
                daily = listOf(
                    DailyMetricObservation(day = "2026-09-01"), // all-null day: zero items, consumed
                    DailyMetricObservation(day = "2026-09-02", recovery = 78.0),
                ),
                sleep = listOf(
                    SleepSessionObservation("sleep:d:1000", 1000, 2000, 0.9, 50, 40.0, false, null),
                    SleepSessionObservation("sleep:d:2000", 2000, 3000, 0.8, 52, 41.0, true, null),
                ),
                rr = listOf(
                    RrIntervalObservation("rr:d:10:800:0", 10, 800, 0),
                    RrIntervalObservation("rr:d:11:810:0", 11, 810, 0),
                ),
            ),
            api,
        )

        assertTrue(h.pass() is EnginePassResult.Drained)
        assertEquals(listOf("daily", "sleep", "rr"), api.order) // frozen family order, hr empty
        assertEquals("2026-09-02", h.wm.lastAckedDailyDay())
        assertEquals(2000L, h.wm.lastAckedSleepTs())
        assertEquals(11L, h.wm.lastAckedRrTs())
        assertEquals(0L, h.wm.lastAckedHrTs()) // no HR rows ever shipped

        // The all-null day produced zero items; the day with recovery shipped exactly one item.
        assertEquals(1, api.dailyReqs.single().items.size)
        assertEquals(listOf("2026-09-02" to 78.0), api.dailyReqs.single().items.map { it.day to it.value })

        assertEquals(2, api.sleepReqs.single().sessions.size)
        assertTrue(api.sleepReqs.single().sessions.any { it.userEdited })

        assertEquals(2, api.rrReqs.single().records.size)
        assertTrue(h.queue.loadAll().all { it.state == QueueEntryState.ACKED })
    }

    @Test
    fun `retryable response keeps the watermark and re-queues under the same batch id`() {
        val api = StubApi().apply { dailyResult = { SyncCallResult.Retryable("NETWORK", "down") } }
        val h = harness(
            FakeSource(daily = listOf(DailyMetricObservation(day = "2026-09-01", recovery = 1.0))),
            api,
        )

        assertTrue(h.pass() is EnginePassResult.RetryLater)
        assertEquals("", h.wm.lastAckedDailyDay()) // NOT advanced — advance happens only on an ack
        val pending = h.queue.loadAll().single()
        assertEquals(QueueEntryState.PENDING, pending.state)
        assertTrue(pending.nextAttemptAtMs > h.nowMs)

        // Step past the backoff, recover the network: the retry ships under the SAME batch id.
        h.nowMs = pending.nextAttemptAtMs + 1
        api.dailyResult = { SyncCallResult.Ok(ack(it)) }
        assertTrue(h.pass() is EnginePassResult.Drained)
        assertEquals("2026-09-01", h.wm.lastAckedDailyDay())
        assertEquals(2, api.dailyReqs.size) // the failed attempt + the retry
        assertEquals(pending.batchId, api.dailyReqs.last().batchId)
        assertEquals(1, h.queue.loadAll().count { it.state == QueueEntryState.ACKED })
    }

    @Test
    fun `rejected family batch parks its window without halting the other families`() {
        val api = StubApi().apply { dailyResult = { SyncCallResult.Ok(ack(it, accepted = false)) } }
        val h = harness(
            FakeSource(
                daily = listOf(DailyMetricObservation(day = "2026-09-01", recovery = 1.0)),
                sleep = listOf(SleepSessionObservation("sleep:d:1000", 1000, 2000, 0.9, 50, 40.0, false, null)),
            ),
            api,
        )

        assertTrue(h.pass() is EnginePassResult.Drained)
        assertEquals(listOf("daily", "sleep"), api.order) // sleep drained despite the daily rejection
        assertEquals("2026-09-01", h.wm.lastAckedDailyDay()) // skipped: window consumed, loudly
        assertEquals(1000L, h.wm.lastAckedSleepTs())
        assertEquals(
            listOf(QueueEntryState.ACKED, QueueEntryState.FAILED_PERMANENT), // sorted by name
            h.queue.loadAll().map { it.state }.sortedBy { it.name },
        )
    }

    @Test
    fun `invalid credentials halt the pass and keep the family batch pending`() {
        val api = StubApi().apply { rrResult = { SyncCallResult.CredentialsInvalid("DEVICE_REVOKED", "revoked") } }
        val h = harness(FakeSource(rr = listOf(RrIntervalObservation("rr:d:10:800:0", 10, 800, 0))), api)

        assertTrue(h.pass() is EnginePassResult.Halted)
        assertEquals(0L, h.wm.lastAckedRrTs()) // no ack → no advance
        assertEquals(QueueEntryState.PENDING, h.queue.loadAll().single().state)
    }

    @Test
    fun `rr backlog chunks at the 20k batch cap with boundary re-read`() {
        val api = StubApi()
        val h = harness(FakeSource(rr = (1..20_001).map { RrIntervalObservation("rr:d:$it:900:0", it.toLong(), 900, 0) }), api)

        assertTrue(h.pass() is EnginePassResult.Drained)
        assertEquals(2, api.rrReqs.size)
        assertEquals(SyncContract.RR_MAX_RECORDS_PER_BATCH, api.rrReqs[0].records.size)
        assertEquals(20_000L, isoToEpoch(api.rrReqs[0].records.last().ts))
        // The second batch re-sends the boundary record (server dedupe) plus the tail.
        assertEquals(2, api.rrReqs[1].records.size)
        assertEquals("rr:d:20000:900:0", api.rrReqs[1].records.first().sourceRecordId)
        assertEquals(20_001L, h.wm.lastAckedRrTs())
    }

    private fun isoToEpoch(ts: String): Long = java.time.Instant.parse(ts).epochSecond
}
