package com.noop.sync

import com.noop.sync.dto.DailyObservationItemDto
import com.noop.sync.dto.IngestRecordDto
import com.noop.sync.dto.RrIntervalRecordDto
import com.noop.sync.dto.SleepSessionDto
import com.noop.sync.dto.SleepStageDto
import java.io.File
import java.time.Instant
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.longOrNull

/*
 * Observation source + sync watermark + family mappers (fork addition).
 *
 * WHY A WATERMARK, NOT THE `synced` COLUMN: recon on NOOP's schema shows `hrSample.synced` is a
 * per-row flag added in v5 "for schema parity" with the Swift store — NOTHING in NOOP ever writes
 * it to 1, no DAO method exists to do so, and the PPG-union read hardcodes 0 for derived rows
 * (see docs/somatriq/SYNC-NOTES.md). It cannot serve as a sync watermark, and writing to NOOP's
 * schema-locked tables from the sync module would fight the Room<->GRDB oracle. Instead each
 * family's last-ACKED key lives in this module's own state file — monotonic, atomically persisted,
 * and invisible to NOOP.
 *
 * The source interface inverts the dependency: :app depends on :sync, so the app supplies an
 * adapter over its own Room DAO (see SomatriqSyncBridge in the app module). HrSample ships since
 * the first slice; dailyMetric / sleepSession / rrInterval join as the same interface's family
 * reads. All reads are READ-ONLY — the interface has no write method by construction.
 *
 * FAMILY WATERMARK KEYS (each advances ONLY on an accepted ack, never backwards):
 *   dailyMetric   → max synced `day`   (NOOP stores LOCAL "yyyy-MM-dd"; lexicographic == chronological)
 *   sleepSession  → max synced `startTs` (unix seconds; unique per row — PK (deviceId, startTs))
 *   rrInterval    → max synced `ts`      (unix seconds; NOT unique per row — PK (deviceId, ts, rrMs, seq),
 *                                         so windows re-read from the boundary ts INCLUSIVE and the
 *                                         server dedupes re-sent boundary rows by source_record_id)
 */

/** One decoded, persisted observation handed over by the host app. */
data class HrObservation(
    /** Stable, unique per row — e.g. "hrSample:<deviceId>:<ts>". Server dedupes on this. */
    val sourceRecordId: String,
    val tsEpochSeconds: Long,
    val bpm: Int,
)

/**
 * One NOOP `dailyMetric` row, mirrored field-for-field (nullable columns nullable here — the
 * entity lives at com.noop.data.Entities.kt DailyMetric). A null column becomes an OMITTED wire
 * item, never a null value. Field names match the entity EXACTLY so the app-side adapter is a
 * 1:1 copy and the [toItems] mapping table below stays auditable against SyncContract.DAILY_METRICS.
 */
data class DailyMetricObservation(
    val day: String,
    val totalSleepMin: Double? = null,
    val efficiency: Double? = null,
    val deepMin: Double? = null,
    val remMin: Double? = null,
    val lightMin: Double? = null,
    val disturbances: Int? = null,
    val restingHr: Int? = null,
    val avgHrv: Double? = null,
    val recovery: Double? = null,
    val strain: Double? = null,
    val exerciseCount: Int? = null,
    val spo2Pct: Double? = null,
    val skinTempDevC: Double? = null,
    val respRateBpm: Double? = null,
    val steps: Int? = null,
    val activeKcalEst: Double? = null,
    val avgSdnn: Double? = null,
    val skinTempC: Double? = null,
)

/**
 * One stage span of a session's hypnogram, as NOOP stores it in `sleepSession.stagesJSON`:
 * `state` is the ON-DEVICE vocabulary "wake"|"light"|"deep"|"rem" (strings, not codes), times are
 * wall-clock unix SECONDS. The wire vocabulary renames "wake" → "awake" in [SyncRepository].
 */
data class SleepStageSpan(
    val startEpochSeconds: Long,
    val endEpochSeconds: Long,
    val state: String,
)

/** One NOOP `sleepSession` row ready to ship (the adapter only hands over FINISHED sessions). */
data class SleepSessionObservation(
    /** Stable, unique per row — "sleep:<deviceId>:<startTs>". Server dedupes/upserts on this. */
    val sourceRecordId: String,
    val startTsEpochSeconds: Long,
    val endTsEpochSeconds: Long,
    /** 0..1 BOTH in NOOP and on the wire (verified — SYNC-NOTES §3.2; passed through unchanged). */
    val efficiency: Double?,
    val restingHr: Int?,
    val avgHrv: Double?,
    val userEdited: Boolean,
    /** Raw NOOP stagesJSON; parsed by [StagesJson] here so the mapping is unit-tested in :sync. */
    val stagesJSON: String?,
)

/**
 * One NOOP `rrInterval` row. `ts` is unix SECONDS (NOT ms — same wall-clock grid as HrSample, see
 * SYNC-NOTES §3.4). `seq` alone is NOT unique (it counts per (ts, rrMs) within one insert batch),
 * so the id must carry the full PK.
 */
data class RrIntervalObservation(
    /** Stable, unique per row — "rr:<deviceId>:<ts>:<rrMs>:<seq>" (the Room PK). */
    val sourceRecordId: String,
    val tsEpochSeconds: Long,
    val rrMs: Int,
    val seq: Int,
)

/**
 * Parser of NOOP's on-device `stagesJSON` (mirror of AnalyticsEngine.encodeStages' output shape;
 * see SYNC-NOTES §3.3). Only the SEGMENT-ARRAY shape decodes:
 *
 *   [{"end":1758611220,"stage":"deep","start":1758611160}, …]   (unix seconds, keys any order)
 *
 * The imported minute-dict shape {"light":…,"deep":…,"rem":…,"awake":…} is not a timeline and
 * yields an empty list (the session still ships, with `"stages":[]`). Anything malformed yields an
 * empty list, exactly like AnalyticsEngine.decodeStages — sync must never crash on a weird row.
 */
object StagesJson {

    private val lenient = Json { ignoreUnknownKeys = true }

    /** NOOP on-device vocabulary → frozen wire vocabulary (only "wake" differs). */
    private val STATE_TO_WIRE = mapOf("wake" to "awake", "light" to "light", "deep" to "deep", "rem" to "rem")

    fun parse(json: String?): List<SleepStageSpan> {
        if (json.isNullOrBlank()) return emptyList()
        return try {
            val arr = lenient.parseToJsonElement(json)
            if (arr !is JsonArray) return emptyList()
            arr.mapNotNull { el ->
                val o = el as? kotlinx.serialization.json.JsonObject ?: return@mapNotNull null
                val start = (o["start"] as? JsonPrimitive)?.longOrNull ?: return@mapNotNull null
                val end = (o["end"] as? JsonPrimitive)?.longOrNull ?: return@mapNotNull null
                val state = (o["stage"] as? JsonPrimitive)?.content ?: return@mapNotNull null
                if (state.isEmpty()) return@mapNotNull null
                SleepStageSpan(start, end, state)
            }
        } catch (_: Exception) {
            emptyList()
        }
    }

    /** Map to the frozen wire states; an unknown state string drops the span (never invents one). */
    fun toWireStages(json: String?): List<SleepStageDto> =
        parse(json).mapNotNull { s ->
            val wire = STATE_TO_WIRE[s.state] ?: return@mapNotNull null
            SleepStageDto(
                state = wire,
                startTs = SyncRepository.formatTs(s.startEpochSeconds),
                endTs = SyncRepository.formatTs(s.endEpochSeconds),
            )
        }
}

/** Read access to observations the host app has already persisted locally. */
interface ObservationSource {
    /**
     * Measured HR rows with `ts` in (fromTsExclusive, toTsInclusive], ascending, at most [limit].
     * Reads ONLY — the sync layer never writes NOOP's tables.
     */
    suspend fun hrSamples(fromTsExclusive: Long, toTsInclusive: Long, limit: Int): List<HrObservation>

    /** Highest persisted observation ts, or null when nothing exists yet (first sync sends all). */
    suspend fun maxHrTs(): Long?

    /**
     * The device's LOCAL "yyyy-MM-dd" for today, in the same format NOOP computes `dailyMetric.day`
     * — or null when unknown. The CURRENT day's metrics row keeps mutating until the day is over,
     * so rows at day >= today are held back until tomorrow (see SYNC-NOTES §3.1); null disables the
     * hold-back and ships everything found.
     */
    suspend fun todayDay(): String?

    /**
     * Daily-metric rows with day >= [fromDayInclusive] and day < today (when known), ascending by
     * day, at most [limit]. The lower bound is INCLUSIVE on purpose: the boundary row re-reads on
     * the next window and the server dedupes it.
     */
    suspend fun dailyMetrics(fromDayInclusive: String, limit: Int): List<DailyMetricObservation>

    /**
     * Sleep sessions with startTs >= [fromStartTsInclusive], ascending, at most [limit] — and only
     * sessions the adapter considers FINISHED (endTs in the past). Lower bound inclusive (boundary
     * re-read; server dedupe).
     */
    suspend fun sleepSessions(fromStartTsInclusive: Long, limit: Int): List<SleepSessionObservation>

    /**
     * R-R rows with ts >= [fromTsInclusive], ascending by (ts, ord, rrMs, seq), at most [limit].
     * The source applies NOOP's own read filters (excludes the redundant SPO2_IBI channel and
     * future-stamped tsSuspect rows) so sync sees exactly what local scoring sees.
     */
    suspend fun rrIntervals(fromTsInclusive: Long, limit: Int): List<RrIntervalObservation>
}

/** Maps observations to the frozen wire DTOs. Pure; no Android framework. */
class SyncRepository(private val source: ObservationSource) {

    suspend fun records(fromTsExclusive: Long, toTsInclusive: Long, limit: Int): List<IngestRecordDto> =
        source.hrSamples(fromTsExclusive, toTsInclusive, limit)
            .map { o ->
                IngestRecordDto(
                    sourceRecordId = o.sourceRecordId,
                    ts = formatTs(o.tsEpochSeconds),
                    bpm = o.bpm.toDouble(),
                )
            }

    suspend fun maxHrTs(): Long? = source.maxHrTs()

    /**
     * The ts that bounds a batch of at most [maxRecords] rows beyond [fromTsExclusive]: the
     * maxRecords-th row's ts when more remain, else the last row's ts, else null (nothing new).
     * Batching by ts-window keeps acked windows exactly aligned with shipped rows.
     */
    suspend fun nextWindowEnd(fromTsExclusive: Long, maxRecords: Int): Long? {
        val rows = source.hrSamples(fromTsExclusive, Long.MAX_VALUE, maxRecords + 1)
        if (rows.isEmpty()) return null
        return if (rows.size > maxRecords) rows[maxRecords - 1].tsEpochSeconds else rows.last().tsEpochSeconds
    }

    // --- daily-observations family -----------------------------------------------------------------

    /**
     * The daily window beyond [fromDayInclusive]: (rows, windowEndDay), or null when nothing NEW
     * exists (rows at the watermark day itself were already acked — they re-read as boundary and
     * ship free, so a fully-acked tail never re-mints a batch). `windowEndDay` is the last
     * FULLY-shipped day: days are unique per row, so the cut never splits a key.
     */
    suspend fun dailyWindow(fromDayInclusive: String, maxRows: Int): Pair<List<DailyMetricObservation>, String>? =
        familyWindow(
            read = source.dailyMetrics(fromDayInclusive, maxRows + 1),
            fromKey = fromDayInclusive,
            maxRows = maxRows,
            keyOf = { it.day },
            stepPastKey = { java.time.LocalDate.parse(it).plusDays(1).toString() }, // unreachable: day is unique per row
        )?.let { (rows, end) -> rows to end }

    /** Send-time read of an already-enqueued daily window: rows with day in [from, to]. */
    suspend fun dailyRowsInWindow(fromDayInclusive: String, toDayInclusive: String): List<DailyMetricObservation> =
        source.dailyMetrics(fromDayInclusive, windowReadLimit(SyncContract.DAILY_MAX_ROWS_PER_BATCH))
            .filter { it.day <= toDayInclusive }

    /** Expand one row into wire items — null columns are OMITTED, per the frozen contract. */
    fun toItems(row: DailyMetricObservation): List<DailyObservationItemDto> = buildList {
        row.totalSleepMin?.let { add(item(row.day, "total_sleep_min", it)) }
        row.efficiency?.let { add(item(row.day, "efficiency", it)) }
        row.deepMin?.let { add(item(row.day, "deep_min", it)) }
        row.remMin?.let { add(item(row.day, "rem_min", it)) }
        row.lightMin?.let { add(item(row.day, "light_min", it)) }
        row.disturbances?.let { add(item(row.day, "disturbances", it.toDouble())) }
        row.restingHr?.let { add(item(row.day, "resting_hr", it.toDouble())) }
        row.avgHrv?.let { add(item(row.day, "avg_hrv", it)) }
        row.recovery?.let { add(item(row.day, "recovery", it)) }
        row.strain?.let { add(item(row.day, "strain", it)) }
        row.exerciseCount?.let { add(item(row.day, "exercise_count", it.toDouble())) }
        row.spo2Pct?.let { add(item(row.day, "spo2_pct", it)) }
        row.skinTempDevC?.let { add(item(row.day, "skin_temp_dev_c", it)) }
        row.respRateBpm?.let { add(item(row.day, "resp_rate_bpm", it)) }
        row.steps?.let { add(item(row.day, "steps", it.toDouble())) }
        row.activeKcalEst?.let { add(item(row.day, "active_kcal_est", it)) }
        row.avgSdnn?.let { add(item(row.day, "avg_sdnn", it)) }
        row.skinTempC?.let { add(item(row.day, "skin_temp_c", it)) }
    }

    private fun item(day: String, metric: String, value: Double) =
        DailyObservationItemDto(day = day, metric = metric, value = value)

    // --- sleep-sessions family ---------------------------------------------------------------------

    /** Like [dailyWindow], keyed on startTs (unique per row — the cut is exact). */
    suspend fun sleepWindow(
        fromStartTsInclusive: Long,
        maxRows: Int,
    ): Pair<List<SleepSessionObservation>, Long>? =
        familyWindow(
            read = source.sleepSessions(fromStartTsInclusive, maxRows + 1),
            fromKey = fromStartTsInclusive,
            maxRows = maxRows,
            keyOf = { it.startTsEpochSeconds },
            stepPastKey = { it + 1 }, // unreachable: boundary ≤ 1 row (startTs is unique)
        )?.let { (rows, end) -> rows to end }

    /** Send-time read of an already-enqueued sleep window: sessions with startTs in [from, to]. */
    suspend fun sleepRowsInWindow(
        fromStartTsInclusive: Long,
        toStartTsInclusive: Long,
    ): List<SleepSessionObservation> =
        source.sleepSessions(fromStartTsInclusive, windowReadLimit(SyncContract.SLEEP_MAX_SESSIONS_PER_BATCH))
            .filter { it.startTsEpochSeconds <= toStartTsInclusive }

    fun toDto(o: SleepSessionObservation): SleepSessionDto = SleepSessionDto(
        sourceRecordId = o.sourceRecordId,
        startTs = formatTs(o.startTsEpochSeconds),
        endTs = formatTs(o.endTsEpochSeconds),
        efficiency = o.efficiency,
        restingHr = o.restingHr,
        avgHrv = o.avgHrv,
        userEdited = o.userEdited,
        stages = StagesJson.toWireStages(o.stagesJSON),
    )

    // --- rr-intervals family -----------------------------------------------------------------------

    /**
     * The R-R window beyond [fromTsInclusive]. `ts` is NOT unique per row (the PK is
     * (deviceId, ts, rrMs, seq)), so when the window is full it is cut at the last ts that the
     * NEXT row does not share — rows beyond the cut sharing the boundary ts ship in the next
     * window, which re-reads from the boundary ts INCLUSIVE and re-sends the boundary rows it
     * already acked (the server dedupes them by source_record_id).
     */
    suspend fun rrWindow(
        fromTsInclusive: Long,
        maxRows: Int,
    ): Pair<List<RrIntervalObservation>, Long>? =
        familyWindow(
            read = source.rrIntervals(fromTsInclusive, maxRows + 1),
            fromKey = fromTsInclusive,
            maxRows = maxRows,
            keyOf = { it.tsEpochSeconds },
            stepPastKey = { it + 1 }, // pathological only: >maxRows rows sharing the watermark ts
        )?.let { (rows, end) -> rows to end }

    /** Send-time read of an already-enqueued R-R window: rows with ts in [from, to]. */
    suspend fun rrRowsInWindow(fromTsInclusive: Long, toTsInclusive: Long): List<RrIntervalObservation> =
        source.rrIntervals(fromTsInclusive, windowReadLimit(SyncContract.RR_MAX_RECORDS_PER_BATCH))
            .filter { it.tsEpochSeconds <= toTsInclusive }

    fun toDto(o: RrIntervalObservation): RrIntervalRecordDto = RrIntervalRecordDto(
        sourceRecordId = o.sourceRecordId,
        ts = formatTs(o.tsEpochSeconds),
        rrMs = o.rrMs,
        seq = o.seq,
    )

    // --- shared window math ------------------------------------------------------------------------

    /**
     * THE family windowing rule, shared by daily/sleep/rr. Input is the source's cap+1 read of
     * rows ordered ascending by key, starting at [fromKey] INCLUSIVE. Output is (rows-to-ship,
     * windowEndKey), or null when nothing NEW exists.
     *
     * 1. BOUNDARY ROWS SHIP FREE. Rows whose key equals [fromKey] were already acked (the
     *    watermark sits on their key); they re-read here only because the lower bound is
     *    inclusive. If they consumed cap slots, a fully-booked boundary key would leave the cut
     *    with no progress and the engine would re-mint the same window forever — so they ship
     *    without counting against [maxRows], and the server dedupes them.
     * 2. If NOTHING beyond the boundary exists within the read, the family is drained at this
     *    watermark → null (never re-mint a no-progress batch).
     * 3. When more rows remain than the budget, the window is cut at the last key the NEXT row
     *    does not share, so no key is ever split by a window boundary; rows sharing the cut key
     *    beyond the budget ship next window (as free boundary rows).
     * 4. If the budget would split a whole key group (every budget+1 row of `rest` shares one
     *    key — needs >cap rows on adjacent keys), ship the group capped and END ON the key: the
     *    dropped stragglers re-read as boundary next window. No loss, always progress.
     * 5. [stepPastKey] exists only for the pathological boundary-fills-the-cap case (>maxRows
     *    rows sharing the watermark key itself — >20k beats in ONE second for R-R, unreachable
     *    for unique-key families): ship the cap and step PAST the key, because ending on it would
     *    re-mint forever. Stragglers at that key are skipped, loudly bounded by physics.
     */
    private fun <T, K : Comparable<K>> familyWindow(
        read: List<T>,
        fromKey: K,
        maxRows: Int,
        keyOf: (T) -> K,
        stepPastKey: (K) -> K,
    ): Pair<List<T>, K>? {
        if (read.isEmpty()) return null
        val boundary = read.takeWhile { keyOf(it) == fromKey }
        val rest = read.drop(boundary.size)
        if (rest.isEmpty()) return null // only already-acked boundary rows → nothing new
        val budget = maxRows - boundary.size
        if (budget <= 0) return boundary.take(maxRows) to stepPastKey(fromKey)
        if (rest.size <= budget) return (boundary + rest) to keyOf(rest.last())
        var cut = -1
        for (i in budget - 1 downTo 0) {
            if (keyOf(rest[i]) < keyOf(rest[i + 1])) {
                cut = i
                break
            }
        }
        return if (cut >= 0) {
            (boundary + rest.take(cut + 1)) to keyOf(rest[cut])
        } else {
            // Every budget+1 row of rest shares one key: ship that group (capped), end ON it.
            val groupKey = keyOf(rest.first())
            val group = (boundary + rest.takeWhile { keyOf(it) == groupKey }).take(maxRows)
            group to groupKey
        }
    }

    companion object {
        /** Unix seconds → ISO-8601 UTC instant string ("2026-09-04T06:41:00Z"). */
        fun formatTs(epochSeconds: Long): String = Instant.ofEpochSecond(epochSeconds).toString()

        /**
         * Send-time window re-read limit: the family's own batch cap plus headroom. The window end
         * was cut at enqueue time; new rows may have landed since, so the re-read fetches cap+slack
         * and filters down to the recorded end key — it must never under-read a full window.
         */
        fun windowReadLimit(familyCap: Int): Int = familyCap + 2_000
    }
}

/**
 * Monotonic last-acked watermarkS, persisted atomically as JSON — one per family, each advancing
 * ONLY on a server ack and never backwards (a stale file or clock skew cannot un-ack data).
 * New fields carry defaults, so a pre-family watermark file decodes unchanged.
 */
@Serializable
data class WatermarkState(
    @SerialName("schema") val schema: Int = 1,
    @SerialName("last_acked_hr_ts") val lastAckedHrTs: Long = 0,
    @SerialName("last_acked_daily_day") val lastAckedDailyDay: String = "",
    @SerialName("last_acked_sleep_ts") val lastAckedSleepTs: Long = 0,
    @SerialName("last_acked_rr_ts") val lastAckedRrTs: Long = 0,
)

class SyncWatermark(private val file: File) {

    private val json = Json { ignoreUnknownKeys = false; encodeDefaults = true }

    @Volatile
    private var state: WatermarkState = load()

    fun lastAckedHrTs(): Long = state.lastAckedHrTs
    fun lastAckedDailyDay(): String = state.lastAckedDailyDay
    fun lastAckedSleepTs(): Long = state.lastAckedSleepTs
    fun lastAckedRrTs(): Long = state.lastAckedRrTs

    /** Returns the effective new watermark (monotonic — a lower ts is ignored). */
    @Synchronized
    fun advanceHrTs(ts: Long): Long =
        advanceIfHigher(state.lastAckedHrTs < ts) { it.copy(lastAckedHrTs = ts) }.lastAckedHrTs

    /**
     * Monotonic on the ISO day string: "" < any day, and lexicographic order == chronological
     * order for fixed-width yyyy-MM-dd, so a plain string compare is order-correct.
     */
    @Synchronized
    fun advanceDailyDay(day: String): String =
        advanceIfHigher(state.lastAckedDailyDay < day) { it.copy(lastAckedDailyDay = day) }.lastAckedDailyDay

    @Synchronized
    fun advanceSleepTs(ts: Long): Long =
        advanceIfHigher(state.lastAckedSleepTs < ts) { it.copy(lastAckedSleepTs = ts) }.lastAckedSleepTs

    @Synchronized
    fun advanceRrTs(ts: Long): Long =
        advanceIfHigher(state.lastAckedRrTs < ts) { it.copy(lastAckedRrTs = ts) }.lastAckedRrTs

    private fun advanceIfHigher(shouldAdvance: Boolean, mutate: (WatermarkState) -> WatermarkState): WatermarkState {
        if (!shouldAdvance) return state
        state = mutate(state)
        persist(state)
        return state
    }

    private fun load(): WatermarkState = runCatching {
        if (file.exists()) json.decodeFromString(WatermarkState.serializer(), file.readText()) else WatermarkState()
    }.getOrDefault(WatermarkState())

    private fun persist(s: WatermarkState) {
        runCatching {
            val tmp = File(file.parentFile, file.name + ".tmp")
            tmp.writeText(json.encodeToString(WatermarkState.serializer(), s))
            if (!tmp.renameTo(file)) {
                tmp.copyTo(file, overwrite = true)
                tmp.delete()
            }
            tmp.delete()
        }
    }
}
