package com.noop.sync.dto

import com.noop.sync.SyncContract
import org.junit.Assert.assertTrue
import org.junit.Test

/*
 * Client-side validation gates (fork addition) — mirror the server's: bpm 20.0..250.0, ts must
 * carry a UTC offset, base64 payload cap 8 MiB, schema gates.
 */
class ValidatorTest {

    private fun invalidReason(v: Validation): String? = (v as? Validation.Invalid)?.reason

    // --- bpm bounds ---

    @Test
    fun `bpm accepts the closed 20 to 250 range`() {
        assertTrue(Validators.validateBpm(20.0) is Validation.Valid)
        assertTrue(Validators.validateBpm(58.0) is Validation.Valid)
        assertTrue(Validators.validateBpm(250.0) is Validation.Valid)
    }

    @Test
    fun `bpm rejects out-of-range and non-finite values`() {
        assertTrue(Validators.validateBpm(19.9) is Validation.Invalid)
        assertTrue(Validators.validateBpm(250.1) is Validation.Invalid)
        assertTrue(Validators.validateBpm(0.0) is Validation.Invalid)
        assertTrue(Validators.validateBpm(Double.NaN) is Validation.Invalid)
        assertTrue(Validators.validateBpm(Double.POSITIVE_INFINITY) is Validation.Invalid)
    }

    // --- ts must be a UTC instant; naive rejected ---

    @Test
    fun `ts accepts UTC Z and explicit offsets`() {
        assertTrue(Validators.validateTs("2026-09-04T06:41:00Z") is Validation.Valid)
        assertTrue(Validators.validateTs("2026-09-04T06:41:00+00:00") is Validation.Valid)
        assertTrue(Validators.validateTs("2026-09-04T08:41:00+02:00") is Validation.Valid)
    }

    @Test
    fun `ts rejects naive datetimes`() {
        val naive = Validators.validateTs("2026-09-04T06:41:00")
        assertTrue(naive is Validation.Invalid)
        assertTrue(invalidReason(naive)!!.contains("offset"))
    }

    @Test
    fun `ts rejects garbage`() {
        assertTrue(Validators.validateTs("") is Validation.Invalid)
        assertTrue(Validators.validateTs("yesterday") is Validation.Invalid)
        assertTrue(Validators.validateTs("2026-09-04") is Validation.Invalid)
    }

    // --- 8 MiB base64 payload cap ---

    @Test
    fun `payload size accepts under the cap`() {
        assertTrue(Validators.validatePayloadSize(8 * 1024 * 1024L - 1, 1024) is Validation.Valid)
    }

    @Test
    fun `payload size rejects uncompressed bytes over the cap`() {
        val v = Validators.validatePayloadSize(8L * 1024 * 1024 + 1, 1024)
        assertTrue(v is Validation.Invalid)
        assertTrue(invalidReason(v)!!.contains("uncompressed"))
    }

    @Test
    fun `payload size rejects oversized base64 regardless of uncompressed claim`() {
        val oversizedB64 = (SyncContract.MAX_PAYLOAD_BYTES / 3L * 4L + 1024L).toInt()
        val v = Validators.validatePayloadSize(42, oversizedB64)
        assertTrue(v is Validation.Invalid)
        assertTrue(invalidReason(v)!!.contains("payload_b64"))
    }

    // --- full-batch gates ---

    private fun validRequest() = IngestBatchRequestDto(
        batchId = "3f9d2c1e-8b47-4a92-9c1d-5e6f7a8b9c0d",
        schemaVersion = SyncContract.SCHEMA_VERSION,
        decoderVersion = "noop-android/11.1.1+somatriq",
        records = listOf(
            IngestRecordDto("hrSample:my-whoop:1758734460", "2026-09-04T06:41:00Z", 58.0),
        ),
        raw = RawPayloadDto(
            codec = "zstd", journalVersion = 1, frameCount = 1,
            payloadB64 = "KLUv/QBYpQEA", payloadSha256 = "a".repeat(64), uncompressedBytes = 42,
            firstFrameTs = "2026-09-04T06:40:59Z", lastFrameTs = "2026-09-04T06:41:00Z",
        ),
    )

    @Test
    fun `well-formed batch passes`() {
        assertTrue(Validators.validateIngestBatch(validRequest()) is Validation.Valid)
    }

    @Test
    fun `wrong schema version fails the gate`() {
        val v = Validators.validateIngestBatch(validRequest().copy(schemaVersion = "3"))
        assertTrue(v is Validation.Invalid)
        assertTrue(invalidReason(v)!!.contains("schema_version"))
    }

    @Test
    fun `wrong codec and journal version fail the gate`() {
        val badCodec = Validators.validateIngestBatch(
            validRequest().let { it.copy(raw = it.raw!!.copy(codec = "lz4")) },
        )
        assertTrue(invalidReason(badCodec)!!.contains("codec"))

        val badJournal = Validators.validateIngestBatch(
            validRequest().let { it.copy(raw = it.raw!!.copy(journalVersion = 2)) },
        )
        assertTrue(invalidReason(badJournal)!!.contains("journal_version"))
    }

    @Test
    fun `bad record inside the batch fails with its index`() {
        val v = Validators.validateIngestBatch(
            validRequest().let {
                it.copy(records = it.records + IngestRecordDto("x", "2026-09-04T06:41:00", 58.0))
            },
        )
        assertTrue(v is Validation.Invalid)
        assertTrue(invalidReason(v)!!.contains("records[1]"))
    }

    @Test
    fun `raw-less batch is valid`() {
        val v = Validators.validateIngestBatch(validRequest().copy(raw = null))
        assertTrue(v is Validation.Valid)
    }
}
