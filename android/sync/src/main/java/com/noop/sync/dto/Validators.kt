package com.noop.sync.dto

import com.noop.sync.SyncContract
import java.time.OffsetDateTime
import java.time.format.DateTimeParseException

/*
 * Client-side validators mirroring the server's ingest gates (fork addition).
 *
 * Goal: never SEND a batch the server is guaranteed to reject — fail locally with a precise reason
 * instead of burning a network round-trip and an idempotency slot.
 */

sealed class Validation {
    object Valid : Validation()
    data class Invalid(val reason: String) : Validation()

    companion object {
        fun invalid(reason: String): Validation = Invalid(reason)
    }
}

object Validators {

    /** bpm must lie in [SyncContract.BPM_MIN, SyncContract.BPM_MAX] (inclusive), like the server. */
    fun validateBpm(bpm: Double): Validation =
        when {
            bpm.isNaN() || bpm.isInfinite() -> Validation.invalid("bpm not a finite number: $bpm")
            bpm < SyncContract.BPM_MIN || bpm > SyncContract.BPM_MAX ->
                Validation.invalid("bpm $bpm outside [${SyncContract.BPM_MIN}, ${SyncContract.BPM_MAX}]")
            else -> Validation.Valid
        }

    /**
     * `ts` must be an ISO-8601 instant WITH a UTC offset — "2026-09-04T06:41:00Z" (or +00:00).
     * Naive datetimes ("2026-09-04T06:41:00") are rejected: without an offset the server cannot
     * know the instant and must not guess.
     */
    fun validateTs(ts: String): Validation = when {
        ts.isBlank() -> Validation.invalid("ts blank")
        !hasExplicitOffset(ts) -> Validation.invalid("ts '$ts' has no UTC offset (naive datetime rejected)")
        else -> try {
            OffsetDateTime.parse(ts)
            Validation.Valid
        } catch (_: DateTimeParseException) {
            Validation.invalid("ts '$ts' not a parseable ISO-8601 instant")
        }
    }

    /** base64-decoded payload must fit the server's 8 MiB cap. */
    fun validatePayloadSize(uncompressedBytes: Long, base64Length: Int): Validation = when {
        base64Length < 0 -> Validation.invalid("negative base64 length")
        // base64 inflates 3:4 — guard both so neither representation slips past the cap.
        base64Length > (SyncContract.MAX_PAYLOAD_BYTES / 3L * 4L).toInt() ->
            Validation.invalid("payload_b64 length $base64Length exceeds the 8 MiB cap")
        uncompressedBytes > SyncContract.MAX_PAYLOAD_BYTES ->
            Validation.invalid("uncompressed payload $uncompressedBytes bytes exceeds the 8 MiB cap")
        else -> Validation.Valid
    }

    /** Full-batch gate: schema version, record-level gates, raw-payload gates. */
    fun validateIngestBatch(batch: IngestBatchRequestDto): Validation {
        if (batch.batchId.isBlank()) return Validation.invalid("batch_id blank")
        if (batch.schemaVersion != SyncContract.SCHEMA_VERSION) {
            return Validation.invalid("schema_version ${batch.schemaVersion} != ${SyncContract.SCHEMA_VERSION}")
        }
        if (batch.decoderVersion.isBlank()) return Validation.invalid("decoder_version blank")
        batch.records.forEachIndexed { i, r ->
            validateBpm(r.bpm).let { if (it is Validation.Invalid) return Validation.invalid("records[$i]: ${it.reason}") }
            validateTs(r.ts).let { if (it is Validation.Invalid) return Validation.invalid("records[$i]: ${it.reason}") }
            if (r.sourceRecordId.isBlank()) return Validation.invalid("records[$i]: source_record_id blank")
        }
        batch.raw?.let { raw ->
            if (raw.codec != SyncContract.CODEC_ZSTD) {
                return Validation.invalid("raw.codec '${raw.codec}' != '${SyncContract.CODEC_ZSTD}'")
            }
            if (raw.journalVersion != SyncContract.JOURNAL_VERSION) {
                return Validation.invalid("raw.journal_version ${raw.journalVersion} != ${SyncContract.JOURNAL_VERSION}")
            }
            validatePayloadSize(raw.uncompressedBytes, raw.payloadB64.length).let {
                if (it is Validation.Invalid) return Validation.invalid("raw: ${it.reason}")
            }
            if (raw.payloadSha256.length != 64) return Validation.invalid("raw.payload_sha256 not a sha256 hex string")
        }
        return Validation.Valid
    }

    private fun hasExplicitOffset(ts: String): Boolean {
        // "Z" suffix, or ±hh:mm / ±hhmm after a 'T'. Everything else is naive or malformed.
        if (ts.endsWith("Z") || ts.endsWith("z")) return true
        val t = ts.lastIndexOf('T')
        if (t < 0) return false
        val tail = ts.substring(t + 1)
        return tail.substringAfterLast('+', "").length >= 2 || tail.substringAfterLast('-', "").length >= 2
    }
}
