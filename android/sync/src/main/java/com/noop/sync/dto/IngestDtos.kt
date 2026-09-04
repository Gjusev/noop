package com.noop.sync.dto

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/*
 * Frozen Somatriq ingest wire contract (fork addition). Field names mirror the server 1:1 —
 * snake_case JSON keys via @SerialName. Do NOT rename, reorder semantically, or loosen optionality
 * without a frozen-contract change on the server side.
 *
 * Timestamps are ISO-8601 UTC instants with explicit offset ("2026-09-04T06:41:00Z"); naive
 * datetimes are rejected by the validators in Validators.kt.
 */

/** One decoded observation. [ts] is an ISO instant string; [bpm] a whole-number double (58.0). */
@Serializable
data class IngestRecordDto(
    @SerialName("source_record_id") val sourceRecordId: String,
    @SerialName("ts") val ts: String,
    @SerialName("bpm") val bpm: Double,
)

/**
 * The raw BLE frame journal payload attached to a batch. `payload_b64` is base64 of the zstd-
 * COMPRESSED journal; `payload_sha256` is the sha256 hex of those same COMPRESSED bytes (not the
 * uncompressed stream).
 */
@Serializable
data class RawPayloadDto(
    @SerialName("codec") val codec: String,
    @SerialName("journal_version") val journalVersion: Int,
    @SerialName("frame_count") val frameCount: Long,
    @SerialName("payload_b64") val payloadB64: String,
    @SerialName("payload_sha256") val payloadSha256: String,
    @SerialName("uncompressed_bytes") val uncompressedBytes: Long,
    /** ISO instant of the first frame's capture clock; null when the journal is empty. */
    @SerialName("first_frame_ts") val firstFrameTs: String? = null,
    @SerialName("last_frame_ts") val lastFrameTs: String? = null,
)

/** POST /api/v1/ingest/batches body. `batch_id` is generated ONCE at enqueue and reused on retries. */
@Serializable
data class IngestBatchRequestDto(
    @SerialName("batch_id") val batchId: String,
    @SerialName("schema_version") val schemaVersion: String,
    @SerialName("decoder_version") val decoderVersion: String,
    @SerialName("records") val records: List<IngestRecordDto>,
    /** Null when the batch carries observations only (e.g. segments pruned, or none ready). */
    @SerialName("raw") val raw: RawPayloadDto? = null,
)

/**
 * Server acknowledgement. `raw_ack == true` is the ONLY authorization to prune the raw journal
 * segments of that batch — an observations-only ack never prunes raw frames.
 */
@Serializable
data class IngestAckDto(
    @SerialName("batch_id") val batchId: String,
    @SerialName("accepted") val accepted: Boolean,
    @SerialName("records_received") val recordsReceived: Long,
    @SerialName("records_inserted") val recordsInserted: Long,
    @SerialName("records_duplicate") val recordsDuplicate: Long,
    @SerialName("raw_ack") val rawAck: Boolean,
    @SerialName("raw_frame_count") val rawFrameCount: Long,
    @SerialName("raw_bytes_stored") val rawBytesStored: Long,
    @SerialName("warnings") val warnings: List<String>,
    @SerialName("server_time") val serverTime: String,
)
