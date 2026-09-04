package com.noop.sync

import com.noop.sync.queue.FileSyncQueue
import com.noop.sync.queue.QueueFamily
import java.io.File
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import kotlinx.coroutines.runBlocking

/*
 * Family window math + watermark tests (fork addition). All three families window through the
 * same rule (SyncRepository.familyWindow): read cap+1 rows from the watermark INCLUSIVE, let the
 * already-acked boundary rows ship free, and cut a full window at the last key the next row does
 * not share — so an acked window covers exactly the rows that shipped, and a fully-acked tail
 * never re-mints a batch.
 */
class FamilyWindowingTest {

    @get:Rule
    val tmp = TemporaryFolder()

    private fun watermark() = SyncWatermark(File(tmp.newFolder(), "watermark.json"))

    // --- shared cut rule ---------------------------------------------------------------------------

    @Test
    fun `daily window ships everything when fewer rows than the cap remain`() = runBlocking<Unit> {
        val rows = (1..5).map { dayRow("2026-09-%02d".format(it)) }
        val repo = SyncRepository(Source(daily = rows))
        val (ship, endDay) = repo.dailyWindow("", 10)!!
        assertEquals(5, ship.size)
        assertEquals("2026-09-05", endDay)
    }

    @Test
    fun `daily window cuts at the cap when more rows remain`() = runBlocking<Unit> {
        val rows = (1..10).map { dayRow("2026-09-%02d".format(it)) }
        val repo = SyncRepository(Source(daily = rows))
        val (ship, endDay) = repo.dailyWindow("", 4)!!
        assertEquals(4, ship.size)
        assertEquals("2026-09-04", endDay)
    }

    @Test
    fun `a fully acked tail yields no window - no re-minting`() = runBlocking<Unit> {
        val rows = (1..5).map { dayRow("2026-09-%02d".format(it)) }
        val repo = SyncRepository(Source(daily = rows))
        // Watermark on the last day: the re-read sees only the boundary row → null, NOT a batch.
        assertNull(repo.dailyWindow("2026-09-05", 10))
        assertNull(repo.sleepWindow(3_000, 10))
        assertNull(repo.rrWindow(30, 10))
    }

    @Test
    fun `boundary rows ship free and do not consume cap slots`() = runBlocking<Unit> {
        // Watermark sits on 2026-09-02 (acked). A cap of 2 must still reach PAST the boundary row.
        val rows = (1..4).map { dayRow("2026-09-%02d".format(it)) }
        val repo = SyncRepository(Source(daily = rows))
        val (ship, endDay) = repo.dailyWindow("2026-09-02", 2)!!
        // boundary 09-02 ships free; budget 1 ships 09-03; 09-04 stays for the next window.
        assertEquals(listOf("2026-09-02", "2026-09-03"), ship.map { it.day })
        assertEquals("2026-09-03", endDay)
    }

    @Test
    fun `sleep window is keyed on startTs`() = runBlocking<Unit> {
        val rows = (1..3).map { session(startTs = 1_000L * it) }
        val repo = SyncRepository(Source(sleep = rows))
        val (ship, endTs) = repo.sleepWindow(0, 2)!!
        assertEquals(2, ship.size)
        assertEquals(2_000L, endTs)

        // Re-read from the boundary ships the acked row again (free) plus the tail — dedupe absorbs.
        val (ship2, endTs2) = repo.sleepWindow(2_000, 2)!!
        assertEquals(listOf(2_000L, 3_000L), ship2.map { it.startTsEpochSeconds })
        assertEquals(3_000L, endTs2)
    }

    @Test
    fun `rr window chunks at 20k records`() = runBlocking<Unit> {
        // 20_001 rows with unique ts: the first window ships exactly the 20k cap.
        val unique = (1..20_001).map { rr(ts = it.toLong(), rrMs = 900, seq = 0) }
        val repo = SyncRepository(Source(rr = unique))
        val (ship1, end1) = repo.rrWindow(0, 20_000)!!
        assertEquals(20_000, ship1.size)
        assertEquals(20_000L, end1)

        // …and the boundary row re-reads from end1 INCLUSIVE (server dedupe absorbs it).
        val (ship2, end2) = repo.rrWindow(end1, 20_000)!!
        assertEquals(listOf(20_000L, 20_001L), ship2.map { it.tsEpochSeconds })
        assertEquals(20_001L, end2)
    }

    @Test
    fun `rr window never splits rows sharing one ts across a cut`() = runBlocking<Unit> {
        // Watermark at ts 9 (acked). Rows over ts [10, 11, 11, 12] - the two ts-11 rows are two
        // beats in one second (PK includes rrMs). A cap of 3 must never end a window BETWEEN them.
        val rows = listOf(
            rr(ts = 10, rrMs = 800, seq = 0),
            rr(ts = 11, rrMs = 810, seq = 0),
            rr(ts = 11, rrMs = 820, seq = 0), // same ts, different beat (PK includes rrMs)
            rr(ts = 12, rrMs = 830, seq = 0),
        )
        val repo = SyncRepository(Source(rr = rows))

        // w1: read [10,11,11,12] (cap+1); budget 3 - keys[2]=11 < keys[3]=12, so the cut lands at
        // index 2 and both 11s ship together with 10 (no split).
        val (ship1, end1) = repo.rrWindow(9, 3)!!
        assertEquals(listOf(10L, 11L, 11L), ship1.map { it.tsEpochSeconds })
        assertEquals(11L, end1)

        // w2 from 11 INCLUSIVE: both 11s re-read as free boundary, 12 ships within budget.
        val (ship2, end2) = repo.rrWindow(end1, 3)!!
        assertEquals(listOf(11L, 11L, 12L), ship2.map { it.tsEpochSeconds })
        assertEquals(12L, end2)
        assertNull(repo.rrWindow(end2, 3))
    }

    fun `rr cut backs off when the budget would split a key group`() = runBlocking<Unit> {
        // Cap 3, rows [10, 11, 11, 11, 12]: the budget would split the three 11s, so the cut
        // backs off to ts 10 (w1); the group rule then ships two 11s and ends ON 11 (w2,
        // deferring one); w3's boundary IS the full 11-group, whose degenerate budget<=0 branch
        // ships all of it and steps past the key — the deferred row is rescued there.
        val rows = listOf(
            rr(ts = 10, rrMs = 800, seq = 0),
            rr(ts = 11, rrMs = 810, seq = 0),
            rr(ts = 11, rrMs = 820, seq = 0),
            rr(ts = 11, rrMs = 830, seq = 0),
            rr(ts = 12, rrMs = 840, seq = 0),
        )
        val repo = SyncRepository(Source(rr = rows))
        val (ship1, end1) = repo.rrWindow(9, 3)!!
        assertEquals(listOf(10L), ship1.map { it.tsEpochSeconds })
        assertEquals(10L, end1)

        val (ship2, end2) = repo.rrWindow(end1, 3)!!
        assertEquals(listOf(10L, 11L, 11L), ship2.map { it.tsEpochSeconds })
        assertEquals(11L, end2)

        val (ship3, end3) = repo.rrWindow(end2, 3)!!
        assertEquals(listOf(11L, 11L, 11L), ship3.map { it.tsEpochSeconds })
        assertEquals(12L, end3)

        // Degenerate tail (unreachable at the real 20k cap with ~2 beats/s): a boundary-only
        // read yields no NEW window, so a row stranded at a stepped-past key does not re-ship.
        assertNull(repo.rrWindow(end3, 3))
    }

    @Test
    fun `empty sources yield no windows`() = runBlocking<Unit> {
        val repo = SyncRepository(Source())
        assertNull(repo.dailyWindow("", 10))
        assertNull(repo.sleepWindow(0, 10))
        assertNull(repo.rrWindow(0, 10))
    }

    // --- watermarks ---------------------------------------------------------------------------------

    @Test
    fun `family watermarks advance only forward`() = runBlocking<Unit> {
        val wm = watermark()
        assertEquals("", wm.lastAckedDailyDay())
        assertEquals("2026-09-03", wm.advanceDailyDay("2026-09-03"))
        assertEquals("2026-09-03", wm.advanceDailyDay("2026-01-01")) // backwards ignored
        assertEquals("2026-09-03", wm.advanceDailyDay("2026-09-03")) // same ignored
        assertEquals("2026-10-01", wm.advanceDailyDay("2026-10-01"))

        assertEquals(0L, wm.lastAckedSleepTs())
        assertEquals(100L, wm.advanceSleepTs(100))
        assertEquals(100L, wm.advanceSleepTs(99))
        assertEquals(150L, wm.advanceSleepTs(150))

        assertEquals(0L, wm.lastAckedRrTs())
        assertEquals(7L, wm.advanceRrTs(7))
        assertEquals(7L, wm.advanceRrTs(3))
    }

    @Test
    fun `watermark file written before the families decodes with family defaults`() = runBlocking<Unit> {
        val dir = tmp.newFolder()
        val file = File(dir, "watermark.json")
        file.writeText("""{"schema":1,"last_acked_hr_ts":123}""")
        val wm = SyncWatermark(file)
        assertEquals(123L, wm.lastAckedHrTs())
        assertEquals("", wm.lastAckedDailyDay())
        assertEquals(0L, wm.lastAckedSleepTs())
        assertEquals(0L, wm.lastAckedRrTs())
    }

    @Test
    fun `watermark reloads after restart with all four family keys`() = runBlocking<Unit> {
        val dir = tmp.newFolder()
        val file = File(dir, "watermark.json")
        SyncWatermark(file).apply {
            advanceHrTs(10)
            advanceDailyDay("2026-09-03")
            advanceSleepTs(20)
            advanceRrTs(30)
        }
        val reloaded = SyncWatermark(file)
        assertEquals(10L, reloaded.lastAckedHrTs())
        assertEquals("2026-09-03", reloaded.lastAckedDailyDay())
        assertEquals(20L, reloaded.lastAckedSleepTs())
        assertEquals(30L, reloaded.lastAckedRrTs())
        assertTrue(file.readText().contains("\"last_acked_daily_day\""))
    }

    // --- queue compat --------------------------------------------------------------------------------

    @Test
    fun `queue entries written before families exist decode as HR`() = runBlocking<Unit> {
        val dir = tmp.newFolder("queue")
        File(dir, "legacy.json").writeText(
            """
            {"batch_id":"legacy","created_at_ms":1,"state":"PENDING",
             "obs_from_ts":0,"obs_to_ts":10,"segments":[]}
            """.trimIndent(),
        )
        val queue = FileSyncQueue(dir)
        val legacy = queue.loadAll().single()
        assertEquals(QueueFamily.HR, legacy.family)
        assertEquals(null, legacy.dayFromInclusive)

        val family = queue.enqueueFamily(QueueFamily.DAILY, 0, 0, "2026-01-01", "2026-02-01", nowMs = 2)
        assertEquals(QueueFamily.DAILY, family.family)
        assertEquals("2026-02-01", family.dayToInclusive)
        assertEquals("2026-02-01", queue.update(family).dayToInclusive)
    }

    // --- helpers -------------------------------------------------------------------------------------

    private fun dayRow(day: String) = DailyMetricObservation(day = day, recovery = 1.0)

    private fun session(startTs: Long) = SleepSessionObservation(
        sourceRecordId = "sleep:d:$startTs", startTsEpochSeconds = startTs, endTsEpochSeconds = startTs + 100,
        efficiency = 0.9, restingHr = 50, avgHrv = 40.0, userEdited = false, stagesJSON = null,
    )

    private fun rr(ts: Long, rrMs: Int, seq: Int) =
        RrIntervalObservation(sourceRecordId = "rr:d:$ts:$rrMs:$seq", tsEpochSeconds = ts, rrMs = rrMs, seq = seq)

    /** In-memory source: each family pages forward from the given key over its list. */
    private class Source(
        private val daily: List<DailyMetricObservation> = emptyList(),
        private val sleep: List<SleepSessionObservation> = emptyList(),
        private val rr: List<RrIntervalObservation> = emptyList(),
    ) : ObservationSource {
        override suspend fun hrSamples(fromTsExclusive: Long, toTsInclusive: Long, limit: Int): List<HrObservation> = emptyList()
        override suspend fun maxHrTs(): Long? = null
        override suspend fun todayDay(): String? = null
        override suspend fun dailyMetrics(fromDayInclusive: String, limit: Int): List<DailyMetricObservation> =
            daily.filter { it.day >= fromDayInclusive }.take(limit)
        override suspend fun sleepSessions(fromStartTsInclusive: Long, limit: Int): List<SleepSessionObservation> =
            sleep.filter { it.startTsEpochSeconds >= fromStartTsInclusive }.take(limit)
        override suspend fun rrIntervals(fromTsInclusive: Long, limit: Int): List<RrIntervalObservation> =
            rr.filter { it.tsEpochSeconds >= fromTsInclusive }.take(limit)
    }
}
