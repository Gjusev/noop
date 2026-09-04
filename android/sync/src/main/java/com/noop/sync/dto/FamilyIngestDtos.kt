package com.noop.sync.dto

import com.noop.sync.SyncContract
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/*
 * Frozen Somatriq single-family ingest wire contracts (fork addition). Field names mirror the
 * server 1:1 — snake_case JSON keys via @SerialName, exactly like IngestDtos.kt. Do NOT rename,
 * reorder semantically, or loosen optionality without a frozen-contract change server-side.
 *
 * All three endpoints share schema_version "1" (they froze independently of /ingest/batches' "2")
 * and ack with the same IngestAckDto envelope (raw_ack is always false for these families — none
 * of them carries raw frames).
 *
 * NULL POLICY: the contract carries no nulls anywhere. A daily metric column that is null becomes
 * an OMITTED item; a sleep-session scalar that is null (efficiency/resting_hr/avg_hrv are nullable
 * in NOOP) is an OMITTED key, never `"efficiency": null`. Omission on encode is what
 * [FamilyJson]'s explicitNulls = false buys; [DtoJson] stays untouched because its null-emission
 * behavior is itself part of the frozen /ingest/batches contract.
 */

/** Derived from the frozen strict instance; omits null optional keys instead of emitting nulls. */
object FamilyJson {
    val json: Json = Json(from = DtoJson.json) { explicitNulls = false }
}

// --- POST /api/v1/ingest/daily-observations -------------------------------------------------------------

/** One (day, metric, value) observation. `metric` ∈ [SyncContract.DAILY_METRICS]; nulls are omitted ITEMS. */
@Serializable
data class DailyObservationItemDto(
    @SerialName("day") val day: String,
    @SerialName("metric") val metric: String,
    @SerialName("value") val value: Double,
)

@Serializable
data class DailyObservationsRequestDto(
    @SerialName("batch_id") val batchId: String,
    @SerialName("schema_version") val schemaVersion: String,
    @SerialName("decoder_version") val decoderVersion: String,
    @SerialName("items") val items: List<DailyObservationItemDto>,
)

// --- POST /api/v1/ingest/sleep-sessions ------------------------------------------------------------------

/**
 * One stage segment of a session's hypnogram. `state` ∈ [SyncContract.SLEEP_STATES]
 * (awake|light|deep|rem — NOOP's on-device vocabulary says "wake" for the first one; the mapper
 * in SyncRepository renames it, never the reverse).
 */
@Serializable
data class SleepStageDto(
    @SerialName("state") val state: String,
    @SerialName("start_ts") val startTs: String,
    @SerialName("end_ts") val endTs: String,
)

/**
 * One sleep session. `efficiency` is 0..1 (NOOP stores the same scale — verified, see
 * SYNC-NOTES §3.2; no normalization is applied). Nullable scalars are OMITTED when NOOP has none.
 */
@Serializable
data class SleepSessionDto(
    @SerialName("source_record_id") val sourceRecordId: String,
    @SerialName("start_ts") val startTs: String,
    @SerialName("end_ts") val endTs: String,
    @SerialName("efficiency") val efficiency: Double? = null,
    @SerialName("resting_hr") val restingHr: Int? = null,
    @SerialName("avg_hrv") val avgHrv: Double? = null,
    @SerialName("user_edited") val userEdited: Boolean,
    @SerialName("stages") val stages: List<SleepStageDto>,
)

@Serializable
data class SleepSessionsRequestDto(
    @SerialName("batch_id") val batchId: String,
    @SerialName("schema_version") val schemaVersion: String,
    @SerialName("decoder_version") val decoderVersion: String,
    @SerialName("sessions") val sessions: List<SleepSessionDto>,
)

// --- POST /api/v1/ingest/rr-intervals --------------------------------------------------------------------

/** One R-R interval. `rr_ms` ∈ [SyncContract.RR_MS_MIN, SyncContract.RR_MS_MAX] (200..2500). */
@Serializable
data class RrIntervalRecordDto(
    @SerialName("source_record_id") val sourceRecordId: String,
    @SerialName("ts") val ts: String,
    @SerialName("rr_ms") val rrMs: Int,
    @SerialName("seq") val seq: Int,
)

@Serializable
data class RrIntervalsRequestDto(
    @SerialName("batch_id") val batchId: String,
    @SerialName("schema_version") val schemaVersion: String,
    @SerialName("decoder_version") val decoderVersion: String,
    @SerialName("records") val records: List<RrIntervalRecordDto>,
)
