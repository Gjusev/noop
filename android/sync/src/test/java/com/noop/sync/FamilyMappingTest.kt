package com.noop.sync

import com.noop.sync.dto.Validation
import com.noop.sync.dto.Validators
import java.time.Instant
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

/*
 * NOOP-column → wire mapping tests (fork addition). The daily-metric table, the stagesJSON format,
 * and the efficiency scale are all documented from NOOP's own code/tests (see SYNC-NOTES §3):
 *   - stagesJSON example below is OuraSleepSessionMappingTest's pinned byte string verbatim
 *     (android/app/src/test/java/com/noop/oura/OuraSleepSessionMappingTest.kt:49-56)
 *   - efficiency 0..1 is asserted there as 4 asleep epochs of 5 → 0.8, and the strap path stores
 *     DetectedSleep.efficiency, documented "asleep / in-bed in [0, 1]" (AnalyticsModels.kt:77)
 */
class FamilyMappingTest {

    private val repo = SyncRepository(FakeSource())

    // --- daily columns → wire metrics ------------------------------------------------------------

    @Test
    fun `every NOOP daily column maps to its frozen wire metric, nulls omitted`() {
        val row = DailyMetricObservation(
            day = "2026-09-03",
            totalSleepMin = 452.0,
            efficiency = 0.88,
            deepMin = 91.0,
            remMin = 112.0,
            lightMin = 249.0,
            disturbances = 4,
            restingHr = 52,
            avgHrv = 41.5,
            recovery = 78.0,
            strain = 8.7,
            exerciseCount = 1,
            spo2Pct = 96.5,
            skinTempDevC = -0.3,
            respRateBpm = 14.2,
            steps = 8214,
            activeKcalEst = 412.0,
            avgSdnn = 62.0,
            skinTempC = 33.4,
        )
        val items = repo.toItems(row)
        assertEquals(18, items.size)
        assertEquals(
            listOf(
                "total_sleep_min" to 452.0,
                "efficiency" to 0.88,
                "deep_min" to 91.0,
                "rem_min" to 112.0,
                "light_min" to 249.0,
                "disturbances" to 4.0,
                "resting_hr" to 52.0,
                "avg_hrv" to 41.5,
                "recovery" to 78.0,
                "strain" to 8.7,
                "exercise_count" to 1.0,
                "spo2_pct" to 96.5,
                "skin_temp_dev_c" to -0.3,
                "resp_rate_bpm" to 14.2,
                "steps" to 8214.0,
                "active_kcal_est" to 412.0,
                "avg_sdnn" to 62.0,
                "skin_temp_c" to 33.4,
            ),
            items.map { it.metric to it.value },
        )
        assertTrue(items.all { it.day == "2026-09-03" })
        // The wire set and the frozen vocabulary are the same set.
        assertTrue(items.map { it.metric }.toSet() == com.noop.sync.SyncContract.DAILY_METRICS.toSet())
    }

    @Test
    fun `a null-heavy NOOP day maps to only the present items`() {
        val items = repo.toItems(DailyMetricObservation(day = "2026-01-01", restingHr = 55, steps = 10))
        assertEquals(listOf("resting_hr" to 55.0, "steps" to 10.0), items.map { it.metric to it.value })
        assertTrue(items.none { it.value == 0.0 && it.metric !in setOf("steps", "resting_hr") })
    }

    @Test
    fun `mapped items always pass the frozen validator`() {
        val row = DailyMetricObservation(
            day = "2026-09-03", recovery = 78.0, skinTempDevC = -0.3, disturbances = 4, steps = 8214,
        )
        val request = com.noop.sync.dto.DailyObservationsRequestDto(
            batchId = "b", schemaVersion = "1", decoderVersion = "noop-android/1+somatriq",
            items = repo.toItems(row),
        )
        assertEquals(Validation.Valid, Validators.validateDailyObservations(request))
    }

    // --- stagesJSON → wire stages ------------------------------------------------------------------

    /** Byte-identical to the pinned stagesJson in OuraSleepSessionMappingTest.adjacentEqualStagesMergeIntoOneSegment. */
    private val realStagesJson = "[" +
        "{\"start\":1700000000,\"end\":1700000060,\"stage\":\"deep\"}," +
        "{\"start\":1700000060,\"end\":1700000090,\"stage\":\"light\"}," +
        "{\"start\":1700000090,\"end\":1700000120,\"stage\":\"rem\"}," +
        "{\"start\":1700000120,\"end\":1700000150,\"stage\":\"wake\"}" +
        "]"

    @Test
    fun `real NOOP stagesJSON maps to wire stages, wake renamed to awake`() {
        val stages = StagesJson.toWireStages(realStagesJson)
        assertEquals(4, stages.size)
        assertEquals("deep", stages[0].state)
        assertEquals("light", stages[1].state)
        assertEquals("rem", stages[2].state)
        assertEquals("awake", stages[3].state) // NOOP "wake" → wire "awake"
        assertEquals("2023-11-14T22:13:20Z", stages[0].startTs)
        assertEquals("2023-11-14T22:14:20Z", stages[0].endTs)
        assertEquals(Instant.ofEpochSecond(1_700_000_150).toString(), stages[3].endTs)
    }

    @Test
    fun `the imported minute-dict stagesJSON shape is not a timeline and yields no stages`() {
        val minuteDict = "{\"light\":120,\"deep\":60,\"rem\":90,\"awake\":30}"
        assertTrue(StagesJson.parse(minuteDict).isEmpty())
        assertTrue(StagesJson.toWireStages(minuteDict).isEmpty())
    }

    @Test
    fun `malformed or null stagesJSON yields no stages, never a crash`() {
        assertTrue(StagesJson.toWireStages(null).isEmpty())
        assertTrue(StagesJson.toWireStages("").isEmpty())
        assertTrue(StagesJson.toWireStages("[{\"stage\":\"deep\"}]").isEmpty()) // no start/end
        assertTrue(StagesJson.toWireStages("not json at all").isEmpty())
        assertTrue(StagesJson.toWireStages("[{\"start\":1,\"end\":2,\"stage\":\"nonsense\"}]").isEmpty())
    }

    // --- sleep session mapping ---------------------------------------------------------------------

    @Test
    fun `sleep session maps with efficiency passed through on the same 0-1 scale`() {
        // 4 asleep epochs of 5 → 0.8, exactly OuraSleepSessionMappingTest.efficiencyIsAsleepOverInBed.
        val dto = repo.toDto(
            SleepSessionObservation(
                sourceRecordId = "sleep:my-whoop:1700000000",
                startTsEpochSeconds = 1_700_000_000L,
                endTsEpochSeconds = 1_700_000_150L,
                efficiency = 0.8,
                restingHr = 52,
                avgHrv = 41.5,
                userEdited = true,
                stagesJSON = realStagesJson,
            ),
        )
        assertEquals("sleep:my-whoop:1700000000", dto.sourceRecordId)
        assertEquals("2023-11-14T22:13:20Z", dto.startTs)
        assertEquals("2023-11-14T22:15:50Z", dto.endTs)
        assertEquals(0.8, dto.efficiency!!, 0.0) // stored 0..1, wire 0..1 — no normalization
        assertEquals(52, dto.restingHr)
        assertEquals(41.5, dto.avgHrv!!, 0.0)
        assertEquals(true, dto.userEdited)
        assertEquals(4, dto.stages.size)
        assertEquals(Validation.Valid, Validators.validateSleepSessions(stubRequest(dto)))
    }

    @Test
    fun `hr-only night maps with null scalars omitted and empty stages`() {
        val dto = repo.toDto(
            SleepSessionObservation(
                sourceRecordId = "sleep:my-whoop:1700000000",
                startTsEpochSeconds = 1_700_000_000L,
                endTsEpochSeconds = 1_700_003_600L,
                efficiency = null, // #1801: HR-only nights deliberately carry no efficiency
                restingHr = null,
                avgHrv = null,
                userEdited = false,
                stagesJSON = null,
            ),
        )
        assertEquals(null, dto.efficiency)
        assertEquals(null, dto.restingHr)
        assertEquals(null, dto.avgHrv)
        assertTrue(dto.stages.isEmpty())
        assertEquals(Validation.Valid, Validators.validateSleepSessions(stubRequest(dto)))
    }

    private fun stubRequest(dto: com.noop.sync.dto.SleepSessionDto) = com.noop.sync.dto.SleepSessionsRequestDto(
        batchId = "b", schemaVersion = "1", decoderVersion = "noop-android/1+somatriq",
        sessions = listOf(dto),
    )

    // --- rr mapping ---------------------------------------------------------------------------------

    @Test
    fun `rr row maps with epoch-seconds ts and full-PK id`() {
        val dto = repo.toDto(
            RrIntervalObservation(
                sourceRecordId = "rr:my-whoop:1790270460:950:0",
                tsEpochSeconds = 1_790_270_460L,
                rrMs = 950,
                seq = 0,
            ),
        )
        assertEquals("rr:my-whoop:1790270460:950:0", dto.sourceRecordId)
        assertEquals("2026-09-24T17:21:00Z", dto.ts)
        assertEquals(950, dto.rrMs)
        assertEquals(0, dto.seq)
    }

    private class FakeSource : ObservationSource {
        override suspend fun hrSamples(fromTsExclusive: Long, toTsInclusive: Long, limit: Int): List<HrObservation> = emptyList()
        override suspend fun maxHrTs(): Long? = null
        override suspend fun todayDay(): String? = null
        override suspend fun dailyMetrics(fromDayInclusive: String, limit: Int): List<DailyMetricObservation> = emptyList()
        override suspend fun sleepSessions(fromStartTsInclusive: Long, limit: Int): List<SleepSessionObservation> = emptyList()
        override suspend fun rrIntervals(fromTsInclusive: Long, limit: Int): List<RrIntervalObservation> = emptyList()
    }
}
