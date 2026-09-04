package com.noop.sync.capture

import com.noop.sync.SyncContract
import com.noop.sync.dto.DtoJson
import com.noop.sync.dto.RawPayloadDto
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64
import java.util.concurrent.Executor
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

/*
 * Raw Journal v1 tests (fork addition). Pure JVM — zstd via the zstd-jni desktop jar on the test
 * classpath. Everything deterministic: synchronous IO executor + injected clock.
 */
class RawJournalWriterTest {

    @get:Rule
    val tmp = TemporaryFolder()

    private lateinit var store: FileSegmentStore
    private lateinit var writer: RawJournalWriter
    private var nowMs: Long = 1_758_000_000_000L

    private val syncExecutor = Executor { it.run() }

    @Before
    fun setUp() {
        store = FileSegmentStore(tmp.newFolder("raw"))
        writer = RawJournalWriter(store, syncExecutor) { nowMs }
    }

    private fun frame(i: Int, size: Int = 24): ByteArray =
        ByteArray(size) { j -> ((i * 31 + j) and 0xFF).toByte() }

    @Test
    fun `entries round-trip byte-exact through compress and decompress`() {
        val expected = (0 until 50).map { i ->
            val e = JournalEntry(epochMs = nowMs + i, frame = frame(i))
            writer.capture(e.frame, e.epochMs)
            e
        }
        writer.flushNow()

        val claimed = writer.claimReadySegments()
        assertEquals(1, claimed.size)
        val payload = writer.buildRawPayload(claimed)!!

        val decoded = JournalCodec.decodeJournal(JournalCodec.decompress(Base64.getDecoder().decode(payload.payloadB64)))
        assertEquals(expected.size, decoded.size)
        expected.zip(decoded).forEach { (want, got) ->
            assertEquals(want.epochMs, got.epochMs)
            assertArrayEquals(want.frame, got.frame)
        }
        assertEquals(expected.size.toLong(), payload.frameCount)
    }

    @Test
    fun `entry header is u32_be length then u64_be epoch and length excludes the header`() {
        val f = frame(7)
        writer.capture(f, 0x0102030405060708L)
        writer.flushNow()
        val payload = writer.buildRawPayload(writer.claimReadySegments())!!

        val journal = JournalCodec.decompress(Base64.getDecoder().decode(payload.payloadB64))
        val header = ByteBuffer.wrap(journal, 0, JournalCodec.ENTRY_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN)
        assertEquals(f.size.toLong(), header.int.toLong()) // N counts frame bytes only
        assertEquals(0x0102030405060708L, header.long) // epoch preserved, big-endian
        assertArrayEquals(f, journal.copyOfRange(JournalCodec.ENTRY_HEADER_BYTES, JournalCodec.ENTRY_HEADER_BYTES + f.size))
    }

    @Test
    fun `claimReadySegments empties ready pool and reports correct sha256 and frame count`() {
        repeat(10) { writer.capture(frame(it), nowMs + it) }
        writer.flushNow()

        val claimed = writer.claimReadySegments()
        assertEquals(1, claimed.size)
        assertEquals(0, writer.readySegmentCount()) // drained

        val payload = writer.buildRawPayload(claimed)!!
        assertEquals(10L, payload.frameCount)
        // sha256 is of the COMPRESSED bytes (what payload_b64 carries), not the uncompressed stream.
        val compressed = Base64.getDecoder().decode(payload.payloadB64)
        val expectedSha = MessageDigest.getInstance("SHA-256").digest(compressed)
            .joinToString("") { "%02x".format(it) }
        assertEquals(expectedSha, payload.payloadSha256)
        assertEquals(
            (10 * (JournalCodec.ENTRY_HEADER_BYTES + frame(0).size)).toLong(),
            payload.uncompressedBytes,
        )
        assertEquals(Instant.ofEpochMilli(nowMs).toString(), payload.firstFrameTs)
        assertEquals(Instant.ofEpochMilli(nowMs + 9).toString(), payload.lastFrameTs)
    }

    @Test
    fun `rotation by buffered size produces multiple segments concatenated in sequence order`() {
        // Small frames, many captures: with the default 1 MiB rotation threshold a flush-per-test
        // is impractical — instead force three explicit segments and check ordering.
        repeat(5) { writer.capture(frame(it), nowMs + it) }
        writer.flushNow()
        nowMs += 1_000
        repeat(5) { writer.capture(frame(100 + it), nowMs + it) }
        writer.flushNow()
        nowMs += 1_000
        repeat(5) { writer.capture(frame(200 + it), nowMs + it) }
        writer.flushNow()

        val claimed = writer.claimReadySegments()
        assertEquals(3, claimed.size)
        assertEquals(claimed, claimed.sorted()) // sequence names order

        val payload = writer.buildRawPayload(claimed)!!
        val decoded = JournalCodec.decodeJournal(JournalCodec.decompress(Base64.getDecoder().decode(payload.payloadB64)))
        assertEquals(15, decoded.size)
        assertEquals(15L, payload.frameCount)
        // Sequence order: first segment's frames first, last segment's last.
        assertArrayEquals(frame(0), decoded[0].frame)
        assertArrayEquals(frame(4), decoded[4].frame)
        assertArrayEquals(frame(204), decoded[14].frame)
    }

    @Test
    fun `rotation by elapsed time fires at the 5 minute threshold`() {
        writer.capture(frame(1), nowMs)
        writer.capture(frame(2), nowMs + 1) // same segment
        assertEquals(2L, writer.bufferedFrameCount())

        nowMs += SyncContract.SEGMENT_ROTATE_MS // age the open segment past the threshold
        writer.capture(frame(3), nowMs) // appended, THEN the aged buffer rotates out with it
        assertEquals(0L, writer.bufferedFrameCount()) // the new buffer is empty
        assertEquals(1, writer.readySegmentCount())

        val claimed = writer.claimReadySegments()
        assertEquals(1, claimed.size)
        val payload = writer.buildRawPayload(claimed)!!
        assertEquals(3L, payload.frameCount) // frames 1..3 in the rotated segment
    }

    @Test
    fun `rotation by size fires at the 1 MiB threshold`() {
        // 100 KiB frames: ~11 frames cross the 1 MiB (uncompressed) buffer threshold.
        val big = ByteArray(100 * 1024) { (it and 0xFF).toByte() }
        var captured = 0
        while (writer.readySegmentCount() == 0 && captured < 40) {
            writer.capture(big, nowMs + captured)
            captured++
        }
        assertTrue("rotation should have fired by now", writer.readySegmentCount() >= 1)
        assertTrue("expected ~11 frames to fill 1 MiB, got $captured", captured in 2..12)
    }

    @Test
    fun `prune deletes segment files only when explicitly authorized`() {
        writer.capture(frame(1), nowMs)
        writer.flushNow()
        val claimed = writer.claimReadySegments()
        assertEquals(1, store.listSegments().size) // claimed but not pruned: still on disk

        writer.releaseClaims(claimed)
        assertEquals(claimed, writer.claimReadySegments()) // releasable again

        writer.pruneSegments(claimed)
        assertEquals(0, store.listSegments().size)
        assertNull(writer.buildRawPayload(claimed)) // nothing survives
    }

    @Test
    fun `segments persist across a restart of the store`() {
        writer.capture(frame(1), nowMs)
        writer.flushNow()
        val claimed = writer.claimReadySegments()

        val reopened = FileSegmentStore(storeDir())
        assertEquals(1, reopened.listSegments().size)
        // Sequence continues after the restart: no name reuse.
        val next = reopened.nextSegmentName()
        assertTrue(next > reopened.listSegments().last())
    }

    @Test
    fun `payload dto serializes with frozen snake_case field names`() {
        writer.capture(frame(1), nowMs)
        writer.flushNow()
        val payload = writer.buildRawPayload(writer.claimReadySegments())!!
        val json = DtoJson.json.encodeToString(RawPayloadDto.serializer(), payload)
        listOf(
            "\"codec\"", "\"journal_version\"", "\"frame_count\"", "\"payload_b64\"",
            "\"payload_sha256\"", "\"uncompressed_bytes\"", "\"first_frame_ts\"", "\"last_frame_ts\"",
        ).forEach { key -> assertTrue("expected $key in $json", json.contains(key)) }
    }

    private fun storeDir(): File = store.let { tmp.root.resolve("raw") }
}
