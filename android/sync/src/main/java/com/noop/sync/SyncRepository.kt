package com.noop.sync

import com.noop.sync.dto.IngestRecordDto
import java.io.File
import java.time.Instant
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/*
 * Observation source + sync watermark (fork addition).
 *
 * WHY A WATERMARK, NOT THE `synced` COLUMN: recon on NOOP's schema shows `hrSample.synced` is a
 * per-row flag added in v5 "for schema parity" with the Swift store — NOTHING in NOOP ever writes
 * it to 1, no DAO method exists to do so, and the PPG-union read hardcodes 0 for derived rows
 * (see docs/somatriq/SYNC-NOTES.md). It cannot serve as a sync watermark, and writing to NOOP's
 * schema-locked tables from the sync module would fight the Room<->GRDB oracle. Instead the last
 * ACKED observation ts lives in this module's own state file — monotonic, atomically persisted,
 * and invisible to NOOP.
 *
 * The source interface inverts the dependency: :app depends on :sync, so the app supplies an
 * adapter over its own Room DAO (see SomatriqSyncBridge in the app module). Only HrSample ships
 * in this milestone; more streams slot in as additional sources.
 */

/** One decoded, persisted observation handed over by the host app. */
data class HrObservation(
    /** Stable, unique per row — e.g. "hrSample:<deviceId>:<ts>". Server dedupes on this. */
    val sourceRecordId: String,
    val tsEpochSeconds: Long,
    val bpm: Int,
)

/** Read access to observations the host app has already persisted locally. */
interface ObservationSource {
    /**
     * Measured HR rows with `ts` in (fromTsExclusive, toTsInclusive], ascending, at most [limit].
     * Reads ONLY — the sync layer never writes NOOP's tables.
     */
    suspend fun hrSamples(fromTsExclusive: Long, toTsInclusive: Long, limit: Int): List<HrObservation>

    /** Highest persisted observation ts, or null when nothing exists yet (first sync sends all). */
    suspend fun maxHrTs(): Long?
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

    companion object {
        /** Unix seconds → ISO-8601 UTC instant string ("2026-09-04T06:41:00Z"). */
        fun formatTs(epochSeconds: Long): String = Instant.ofEpochSecond(epochSeconds).toString()
    }
}

/**
 * Monotonic last-acked-ts watermark, persisted atomically as JSON. Advances ONLY on a server ack,
 * and never backwards (a stale file or clock skew cannot un-ack data).
 */
@Serializable
data class WatermarkState(
    @SerialName("schema") val schema: Int = 1,
    @SerialName("last_acked_hr_ts") val lastAckedHrTs: Long = 0,
)

class SyncWatermark(private val file: File) {

    private val json = Json { ignoreUnknownKeys = false; encodeDefaults = true }

    @Volatile
    private var state: WatermarkState = load()

    fun lastAckedHrTs(): Long = state.lastAckedHrTs

    /** Returns the effective new watermark (monotonic — a lower ts is ignored). */
    @Synchronized
    fun advanceHrTs(ts: Long): Long {
        if (ts <= state.lastAckedHrTs) return state.lastAckedHrTs
        state = WatermarkState(schema = state.schema, lastAckedHrTs = ts)
        persist(state)
        return state.lastAckedHrTs
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
