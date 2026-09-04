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

    // --- Single-family endpoints (schema "1") -----------------------------------------------------------

    /**
     * `day` must be a real ISO calendar date "yyyy-MM-dd" (exactly what NOOP's DailyMetric.day
     * stores). String compare on this shape is order-correct, which is what the daily watermark
     * relies on.
     */
    fun validateDay(day: String): Validation = when {
        !ISO_DAY_REGEX.matches(day) -> Validation.invalid("day '$day' is not an ISO yyyy-MM-dd date")
        else -> try {
            java.time.LocalDate.parse(day)
            Validation.Valid
        } catch (_: DateTimeParseException) {
            Validation.invalid("day '$day' is not a real calendar date")
        }
    }

    /** efficiency is 0..1 on the wire (and in NOOP — verified, SYNC-NOTES §3.2). */
    fun validateEfficiency(efficiency: Double): Validation = when {
        efficiency.isNaN() || efficiency.isInfinite() ->
            Validation.invalid("efficiency not a finite number: $efficiency")
        efficiency < 0.0 || efficiency > 1.0 ->
            Validation.invalid("efficiency $efficiency outside [0.0, 1.0]")
        else -> Validation.Valid
    }

    fun validateDailyObservations(request: DailyObservationsRequestDto): Validation {
        familyEnvelope(request.batchId, request.schemaVersion, request.decoderVersion)?.let { return it }
        if (request.items.isEmpty()) return Validation.invalid("items empty")
        request.items.forEachIndexed { i, item ->
            validateDay(item.day).let { if (it is Validation.Invalid) return Validation.invalid("items[$i]: ${it.reason}") }
            if (item.metric !in SyncContract.DAILY_METRICS) {
                return Validation.invalid("items[$i]: metric '${item.metric}' not in the frozen vocabulary")
            }
            if (item.value.isNaN() || item.value.isInfinite()) {
                return Validation.invalid("items[$i]: value not a finite number: ${item.value}")
            }
        }
        return Validation.Valid
    }

    fun validateSleepSessions(request: SleepSessionsRequestDto): Validation {
        familyEnvelope(request.batchId, request.schemaVersion, request.decoderVersion)?.let { return it }
        if (request.sessions.isEmpty()) return Validation.invalid("sessions empty")
        request.sessions.forEachIndexed { i, s ->
            if (s.sourceRecordId.isBlank()) return Validation.invalid("sessions[$i]: source_record_id blank")
            validateTs(s.startTs).let { if (it is Validation.Invalid) return Validation.invalid("sessions[$i].start_ts: ${it.reason}") }
            validateTs(s.endTs).let { if (it is Validation.Invalid) return Validation.invalid("sessions[$i].end_ts: ${it.reason}") }
            s.efficiency?.let {
                validateEfficiency(it).let { v -> if (v is Validation.Invalid) return Validation.invalid("sessions[$i]: ${v.reason}") }
            }
            s.restingHr?.let {
                if (it < SyncContract.BPM_MIN.toInt() || it > SyncContract.BPM_MAX.toInt()) {
                    return Validation.invalid("sessions[$i].resting_hr $it outside [${SyncContract.BPM_MIN.toInt()}, ${SyncContract.BPM_MAX.toInt()}]")
                }
            }
            s.avgHrv?.let {
                if (it.isNaN() || it.isInfinite() || it < 0.0) {
                    return Validation.invalid("sessions[$i].avg_hrv not a finite non-negative number: $it")
                }
            }
            s.stages.forEachIndexed { j, st ->
                if (st.state !in SyncContract.SLEEP_STATES) {
                    return Validation.invalid("sessions[$i].stages[$j]: state '${st.state}' not in ${SyncContract.SLEEP_STATES}")
                }
                validateTs(st.startTs).let { if (it is Validation.Invalid) return Validation.invalid("sessions[$i].stages[$j].start_ts: ${it.reason}") }
                validateTs(st.endTs).let { if (it is Validation.Invalid) return Validation.invalid("sessions[$i].stages[$j].end_ts: ${it.reason}") }
            }
        }
        return Validation.Valid
    }

    fun validateRrIntervals(request: RrIntervalsRequestDto): Validation {
        familyEnvelope(request.batchId, request.schemaVersion, request.decoderVersion)?.let { return it }
        if (request.records.isEmpty()) return Validation.invalid("records empty")
        if (request.records.size > SyncContract.RR_MAX_RECORDS_PER_BATCH) {
            return Validation.invalid(
                "records ${request.records.size} exceed the ${SyncContract.RR_MAX_RECORDS_PER_BATCH}-record batch cap",
            )
        }
        request.records.forEachIndexed { i, r ->
            if (r.sourceRecordId.isBlank()) return Validation.invalid("records[$i]: source_record_id blank")
            validateTs(r.ts).let { if (it is Validation.Invalid) return Validation.invalid("records[$i]: ${it.reason}") }
            if (r.rrMs < SyncContract.RR_MS_MIN || r.rrMs > SyncContract.RR_MS_MAX) {
                return Validation.invalid(
                    "records[$i]: rr_ms ${r.rrMs} outside [${SyncContract.RR_MS_MIN}, ${SyncContract.RR_MS_MAX}]",
                )
            }
            if (r.seq < 0) return Validation.invalid("records[$i]: seq ${r.seq} negative")
        }
        return Validation.Valid
    }

    /** Shared envelope gate for the schema-"1" family endpoints. Null = envelope OK. */
    private fun familyEnvelope(batchId: String, schemaVersion: String, decoderVersion: String): Validation? = when {
        batchId.isBlank() -> Validation.invalid("batch_id blank")
        schemaVersion != SyncContract.FAMILY_SCHEMA_VERSION ->
            Validation.invalid("schema_version $schemaVersion != ${SyncContract.FAMILY_SCHEMA_VERSION}")
        decoderVersion.isBlank() -> Validation.invalid("decoder_version blank")
        else -> null
    }

    private fun hasExplicitOffset(ts: String): Boolean {
        // "Z" suffix, or ±hh:mm / ±hhmm after a 'T'. Everything else is naive or malformed.
        if (ts.endsWith("Z") || ts.endsWith("z")) return true
        val t = ts.lastIndexOf('T')
        if (t < 0) return false
        val tail = ts.substring(t + 1)
        return tail.substringAfterLast('+', "").length >= 2 || tail.substringAfterLast('-', "").length >= 2
    }

    private val ISO_DAY_REGEX = Regex("""^\d{4}-\d{2}-\d{2}$""")
}
