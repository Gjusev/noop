package com.noop.sync.dto

import kotlinx.serialization.SerializationException
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

/*
 * Frozen-contract tests for the three single-family endpoints (fork addition). The fixtures under
 * src/test/resources/somatriq are the frozen wire examples, verbatim — a DTO change that drifts a
 * field name, optionality, or snake_case spelling breaks these before it can drift the server.
 */
class FamilyIngestDtoTest {

    private fun resource(name: String): String =
        javaClass.classLoader!!.getResourceAsStream("somatriq/$name")!!
            .bufferedReader().readText()

    @Test
    fun `daily observations request deserializes the frozen example field for field`() {
        val dto = FamilyJson.json.decodeFromString(
            DailyObservationsRequestDto.serializer(),
            resource("daily-observations-request.json"),
        )
        assertTrue(dto.batchId == "a1b2c3d4-1111-4111-8111-abcdefabcdef")
        assertTrue(dto.schemaVersion == "1")
        assertTrue(dto.decoderVersion == "noop-android/11.1.1+somatriq")
        assertTrue(dto.items.size == 3)
        assertTrue(dto.items[0] == DailyObservationItemDto("2026-09-03", "total_sleep_min", 452.0))
        assertTrue(dto.items[1] == DailyObservationItemDto("2026-09-03", "efficiency", 0.88))
        assertTrue(dto.items[2] == DailyObservationItemDto("2026-09-03", "recovery", 78.0))
    }

    @Test
    fun `sleep sessions request deserializes the frozen example field for field`() {
        val dto = FamilyJson.json.decodeFromString(
            SleepSessionsRequestDto.serializer(),
            resource("sleep-sessions-request.json"),
        )
        assertTrue(dto.batchId == "b2c3d4e5-2222-4222-8222-abcdefabcdef")
        assertTrue(dto.schemaVersion == "1")
        val s = dto.sessions[0]
        assertTrue(dto.sessions.size == 1)
        assertTrue(s.sourceRecordId == "sleep:my-whoop:1758601200")
        assertTrue(s.startTs == "2025-09-23T04:20:00Z")
        assertTrue(s.endTs == "2025-09-23T12:32:00Z")
        assertTrue(s.efficiency == 0.88)
        assertTrue(s.restingHr == 52)
        assertTrue(s.avgHrv == 41.5)
        assertTrue(!s.userEdited)
        assertTrue(s.stages.size == 2)
        assertTrue(s.stages[0] == SleepStageDto("deep", "2025-09-23T04:20:00Z", "2025-09-23T04:40:00Z"))
        assertTrue(s.stages[1] == SleepStageDto("awake", "2025-09-23T04:40:00Z", "2025-09-23T04:45:00Z"))
    }

    @Test
    fun `rr intervals request deserializes the frozen example field for field`() {
        val dto = FamilyJson.json.decodeFromString(
            RrIntervalsRequestDto.serializer(),
            resource("rr-intervals-request.json"),
        )
        assertTrue(dto.batchId == "c3d4e5f6-3333-4333-8333-abcdefabcdef")
        assertTrue(dto.schemaVersion == "1")
        assertTrue(dto.records.size == 2)
        assertTrue(dto.records[0] == RrIntervalRecordDto("rr:my-whoop:1790270460:950:0", "2026-09-24T17:21:00Z", 950, 0))
        assertTrue(dto.records[1] == RrIntervalRecordDto("rr:my-whoop:1790270461:902:0", "2026-09-24T17:21:01Z", 902, 1))
    }

    @Test
    fun `serialize then deserialize round-trips every family request`() {
        val daily = FamilyJson.json.decodeFromString(
            DailyObservationsRequestDto.serializer(), resource("daily-observations-request.json"),
        )
        assertTrue(
            daily == FamilyJson.json.decodeFromString(
                DailyObservationsRequestDto.serializer(),
                FamilyJson.json.encodeToString(DailyObservationsRequestDto.serializer(), daily),
            ),
        )

        val sleep = FamilyJson.json.decodeFromString(
            SleepSessionsRequestDto.serializer(), resource("sleep-sessions-request.json"),
        )
        assertTrue(
            sleep == FamilyJson.json.decodeFromString(
                SleepSessionsRequestDto.serializer(),
                FamilyJson.json.encodeToString(SleepSessionsRequestDto.serializer(), sleep),
            ),
        )

        val rr = FamilyJson.json.decodeFromString(
            RrIntervalsRequestDto.serializer(), resource("rr-intervals-request.json"),
        )
        assertTrue(
            rr == FamilyJson.json.decodeFromString(
                RrIntervalsRequestDto.serializer(),
                FamilyJson.json.encodeToString(RrIntervalsRequestDto.serializer(), rr),
            ),
        )
    }

    @Test
    fun `family serialization emits frozen snake_case keys`() {
        val daily = FamilyJson.json.encodeToString(
            DailyObservationsRequestDto.serializer(),
            FamilyJson.json.decodeFromString(
                DailyObservationsRequestDto.serializer(),
                resource("daily-observations-request.json"),
            ),
        )
        assertTrue(daily.contains("\"batch_id\""))
        assertTrue(daily.contains("\"schema_version\""))
        assertTrue(daily.contains("\"decoder_version\""))
        assertTrue(daily.contains("\"items\""))
        assertTrue(daily.contains("\"source_record_id\"").not()) // daily items have no id

        val sleep = FamilyJson.json.encodeToString(
            SleepSessionsRequestDto.serializer(),
            FamilyJson.json.decodeFromString(
                SleepSessionsRequestDto.serializer(),
                resource("sleep-sessions-request.json"),
            ),
        )
        assertTrue(sleep.contains("\"source_record_id\""))
        assertTrue(sleep.contains("\"user_edited\""))
        assertTrue(sleep.contains("\"avg_hrv\""))
        assertTrue(sleep.contains("\"start_ts\""))
        assertTrue(sleep.contains("\"batchId\"").not())
        assertTrue(sleep.contains("\"userEdited\"").not())
    }

    @Test
    fun `null sleep scalars are OMITTED, never emitted as json null`() {
        val session = SleepSessionDto(
            sourceRecordId = "sleep:my-whoop:1700000000",
            startTs = "2023-11-14T22:13:20Z",
            endTs = "2023-11-15T06:13:20Z",
            efficiency = null,
            restingHr = null,
            avgHrv = null,
            userEdited = false,
            stages = emptyList(),
        )
        val request = SleepSessionsRequestDto(
            batchId = "b",
            schemaVersion = "1",
            decoderVersion = "noop-android/1+somatriq",
            sessions = listOf(session),
        )
        val encoded = FamilyJson.json.encodeToString(SleepSessionsRequestDto.serializer(), request)
        assertTrue(encoded.contains("\"efficiency\"").not())
        assertTrue(encoded.contains("\"resting_hr\"").not())
        assertTrue(encoded.contains("\"avg_hrv\"").not())
        assertTrue(encoded.contains("\"user_edited\":false"))
        assertTrue(encoded.contains("\"stages\":[]"))
    }

    @Test
    fun `unknown keys are rejected - strict contract, no silent drift`() {
        val frozen = resource("rr-intervals-request.json")
        val drifted = frozen.replace("\"batch_id\"", "\"unexpected_future_key\": 1, \"batch_id\"")
        assertThrows(SerializationException::class.java) {
            FamilyJson.json.decodeFromString(RrIntervalsRequestDto.serializer(), drifted)
        }
    }

    @Test
    fun `missing required keys are rejected`() {
        val frozen = resource("daily-observations-request.json")
        val missing = frozen.replace("\"schema_version\": \"1\",", "")
        assertThrows(SerializationException::class.java) {
            FamilyJson.json.decodeFromString(DailyObservationsRequestDto.serializer(), missing)
        }
    }
}
