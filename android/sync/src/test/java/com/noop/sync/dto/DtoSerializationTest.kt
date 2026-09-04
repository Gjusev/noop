package com.noop.sync.dto

import kotlinx.serialization.SerializationException
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

/*
 * Frozen-contract tests (fork addition): the fixtures under src/test/resources/somatriq are the
 * frozen wire examples, verbatim. A DTO change that drifts a field name, optionality, or
 * snake_case spelling breaks these tests before it can drift the server contract.
 */
class DtoSerializationTest {

    private fun resource(name: String): String =
        javaClass.classLoader!!.getResourceAsStream("somatriq/$name")!!
            .bufferedReader().readText()

    @Test
    fun `ingest batch request deserializes the frozen example field for field`() {
        val dto = DtoJson.json.decodeFromString(
            IngestBatchRequestDto.serializer(),
            resource("ingest-batch-request.json"),
        )
        assertEquals("3f9d2c1e-8b47-4a92-9c1d-5e6f7a8b9c0d", dto.batchId)
        assertEquals("2", dto.schemaVersion)
        assertEquals("noop-android/11.1.1+somatriq", dto.decoderVersion)
        assertEquals(3, dto.records.size)
        assertEquals("hrSample:my-whoop:1758734460", dto.records[0].sourceRecordId)
        assertEquals("2026-09-04T06:41:00Z", dto.records[0].ts)
        assertEquals(58.0, dto.records[0].bpm, 0.0)
        assertEquals(59.5, dto.records[1].bpm, 0.0)
        val raw = dto.raw!!
        assertEquals("zstd", raw.codec)
        assertEquals(1, raw.journalVersion)
        assertEquals(3L, raw.frameCount)
        assertEquals("KLUv/QBYpQEAeyJhIjoxfQ==", raw.payloadB64)
        assertEquals(
            "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
            raw.payloadSha256,
        )
        assertEquals(42L, raw.uncompressedBytes)
        assertEquals("2026-09-04T06:40:59Z", raw.firstFrameTs)
        assertEquals("2026-09-04T06:41:02Z", raw.lastFrameTs)
    }

    @Test
    fun `ingest ack deserializes the frozen example field for field`() {
        val dto = DtoJson.json.decodeFromString(IngestAckDto.serializer(), resource("ingest-ack.json"))
        assertEquals("3f9d2c1e-8b47-4a92-9c1d-5e6f7a8b9c0d", dto.batchId)
        assertEquals(true, dto.accepted)
        assertEquals(612L, dto.recordsReceived)
        assertEquals(612L, dto.recordsInserted)
        assertEquals(0L, dto.recordsDuplicate)
        assertEquals(true, dto.rawAck)
        assertEquals(612L, dto.rawFrameCount)
        assertEquals(4211L, dto.rawBytesStored)
        assertTrue(dto.warnings.isEmpty())
        assertEquals("2026-09-04T06:41:05Z", dto.serverTime)
    }

    @Test
    fun `pairing request and response deserialize the frozen examples`() {
        val req = DtoJson.json.decodeFromString(
            PairingConfirmRequestDto.serializer(),
            resource("pairing-confirm-request.json"),
        )
        assertEquals("8CHARSUP", req.pairingCode)
        assertEquals("pixel-8", req.deviceName)

        val resp = DtoJson.json.decodeFromString(
            PairingConfirmResponseDto.serializer(),
            resource("pairing-confirm-response.json"),
        )
        assertEquals("5f0c9b8a-1234-4cde-9abc-def012345678", resp.deviceId)
        assertEquals("sqt_dev_example_token_not_a_real_secret", resp.token)
        assertEquals("device", resp.tokenType)
        assertEquals(listOf("ingest.write", "device.read", "sync.read"), resp.scopes)
        assertEquals("2026-09-04T06:30:00Z", resp.issuedAt)
        assertNull(resp.expiresAt)
    }

    @Test
    fun `error envelope deserializes the frozen example`() {
        val dto = DtoJson.json.decodeFromString(ErrorDto.serializer(), resource("error.json"))
        assertEquals("VALIDATION", dto.errorCode)
        assertEquals("records[3].bpm must be between 20.0 and 250.0", dto.message)
        assertTrue(dto.details.isEmpty())
    }

    @Test
    fun `serialize then deserialize round-trips every DTO`() {
        val request = DtoJson.json.decodeFromString(
            IngestBatchRequestDto.serializer(),
            resource("ingest-batch-request.json"),
        )
        val encoded = DtoJson.json.encodeToString(IngestBatchRequestDto.serializer(), request)
        val decoded = DtoJson.json.decodeFromString(IngestBatchRequestDto.serializer(), encoded)
        assertEquals(request, decoded)

        val ack = DtoJson.json.decodeFromString(IngestAckDto.serializer(), resource("ingest-ack.json"))
        assertEquals(
            ack,
            DtoJson.json.decodeFromString(
                IngestAckDto.serializer(),
                DtoJson.json.encodeToString(IngestAckDto.serializer(), ack),
            ),
        )

        // Serialization emits the frozen snake_case keys, never Kotlin property names.
        assertTrue(encoded.contains("\"batch_id\""))
        assertTrue(encoded.contains("\"schema_version\""))
        assertTrue(encoded.contains("\"source_record_id\""))
        assertTrue(encoded.contains("\"payload_sha256\""))
        assertTrue(!encoded.contains("\"batchId\""))
        assertTrue(!encoded.contains("\"schemaVersion\""))
    }

    @Test
    fun `raw is optional and survives a round-trip as null`() {
        val request = IngestBatchRequestDto(
            batchId = "b", schemaVersion = "2", decoderVersion = "noop-android/1+somatriq",
            records = emptyList(), raw = null,
        )
        val encoded = DtoJson.json.encodeToString(IngestBatchRequestDto.serializer(), request)
        val decoded = DtoJson.json.decodeFromString(IngestBatchRequestDto.serializer(), encoded)
        assertNull(decoded.raw)
    }

    @Test
    fun `unknown keys are rejected - strict contract, no silent drift`() {
        val frozen = resource("ingest-ack.json")
        val drifted = frozen.replace("\"batch_id\"", "\"unexpected_future_key\": 1, \"batch_id\"")
        assertTrue(drifted.contains("unexpected_future_key"))
        assertThrows(SerializationException::class.java) {
            DtoJson.json.decodeFromString(IngestAckDto.serializer(), drifted)
        }
    }

    @Test
    fun `missing required keys are rejected`() {
        val frozen = resource("error.json")
        val missing = frozen.replace("\"error_code\": \"VALIDATION\",", "")
        assertThrows(SerializationException::class.java) {
            DtoJson.json.decodeFromString(ErrorDto.serializer(), missing)
        }
    }
}
