package com.noop.sync.capture

import com.noop.sync.SyncContract
import com.noop.sync.dto.RawPayloadDto
import java.io.ByteArrayOutputStream
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicLong

/*
 * Somatriq Raw Journal writer (fork addition).
 *
 * Appends complete raw BLE frames to an in-memory buffer, and rotates that buffer into a durable,
 * zstd-compressed segment file (filesDir/somatriq/raw/seg-<seq>.zst) at ~1 MiB UNCOMPRESSED or
 * 5 minutes, whichever comes first. Segment compression and file writes run on an injected
 * [Executor] so the GATT binder thread that calls [capture] only ever copies bytes under a lock —
 * never compresses, never touches disk.
 *
 * Durability note (deliberate tradeoff, see docs/somatriq/SYNC-NOTES.md): frames buffered in memory
 * between rotations are lost on process death — bounded by 1 MiB / 5 min of frames. Per-frame
 * fsync would tax the BLE thread for data that still exists on the strap until acked.
 *
 * Pruning discipline: segments are deleted ONLY via [pruneSegments], which the sync worker calls
 * exclusively when the server's ack said raw_ack = true for a batch carrying those segments.
 */

class RawJournalWriter(
    private val store: SegmentStore,
    private val ioExecutor: Executor = Executor { it.run() },
    private val clock: () -> Long = System::currentTimeMillis,
) : RawCapture {

    private val lock = Any()

    /** Current in-progress (uncompressed) buffer. Guarded by [lock]. */
    private var buffer = ByteArrayOutputStream(SyncContract.SEGMENT_ROTATE_BYTES / 4)

    /** Frame count and first-capture clock of the in-progress buffer. Guarded by [lock]. */
    private var bufferedFrames = 0L
    private var bufferOpenedAtMs = 0L

    private val writtenFrames = AtomicLong(0)
    private val droppedFrames = AtomicLong(0)

    /**
     * Segment names claimed by an in-flight batch. They stay on disk until acked-and-pruned or
     * released; guarded by [lock].
     */
    private val claimed = LinkedHashSet<String>()

    // --- capture path (GATT binder thread; must be cheap and total) ---

    /** [RawCapture] entry: stamps the frame with the capture clock. */
    override fun capture(frame: ByteArray) = capture(frame, clock())

    /**
     * Append one complete frame with an explicit epoch (injectable for tests). Cheap: copies the
     * frame into the in-memory buffer under a lock; compression/IO happen only on rotation, on
     * the IO executor.
     */
    fun capture(frame: ByteArray, epochMs: Long) {
        val now = epochMs
        val rotate: ByteArrayOutputStream?
        val segmentName: String
        synchronized(lock) {
            if (bufferOpenedAtMs == 0L) bufferOpenedAtMs = now
            JournalCodec.encodeEntry(buffer, now, frame)
            bufferedFrames++
            writtenFrames.incrementAndGet()
            val ageMs = now - bufferOpenedAtMs
            if (buffer.size() >= SyncContract.SEGMENT_ROTATE_BYTES || ageMs >= SyncContract.SEGMENT_ROTATE_MS) {
                rotate = buffer
                segmentName = store.nextSegmentName()
                buffer = ByteArrayOutputStream(SyncContract.SEGMENT_ROTATE_BYTES / 4)
                bufferedFrames = 0L
                bufferOpenedAtMs = 0L
            } else {
                rotate = null
                segmentName = ""
            }
        }
        if (rotate != null) {
            val bytes = rotate.toByteArray()
            ioExecutor.execute {
                try {
                    store.writeSegment(segmentName, JournalCodec.compress(bytes))
                } catch (_: Throwable) {
                    droppedFrames.addAndGet(1)
                    // Segment lost; the in-memory buffer is already gone. Counted, surfaced via
                    // [droppedSegmentCount], never thrown back into the BLE loop.
                }
            }
        }
    }

    /** Force the in-progress buffer out as a segment now (e.g. app going to background). */
    fun flushNow() {
        val rotate: ByteArrayOutputStream?
        val segmentName: String
        synchronized(lock) {
            if (buffer.size() == 0) return
            rotate = buffer
            segmentName = store.nextSegmentName()
            buffer = ByteArrayOutputStream(SyncContract.SEGMENT_ROTATE_BYTES / 4)
            bufferedFrames = 0L
            bufferOpenedAtMs = 0L
        }
        val bytes = rotate!!.toByteArray()
        ioExecutor.execute {
            try {
                store.writeSegment(segmentName, JournalCodec.compress(bytes))
            } catch (_: Throwable) {
                droppedFrames.addAndGet(1)
            }
        }
    }

    // --- batch assembly (sync worker thread) ---

    /**
     * Atomically claim every READY segment (fully written, not already claimed), in sequence order.
     * Claimed segments are frozen for this batch: they are neither re-claimed by a later batch nor
     * readable as "ready", and they remain on disk until [pruneSegments] or [releaseClaims].
     *
     * Returns the claimed names (empty list when nothing is ready).
     */
    fun claimReadySegments(maxBytes: Long = SyncContract.MAX_PAYLOAD_BYTES): List<String> {
        synchronized(lock) {
            val ready = store.listSegments().filter { it !in claimed }
            val out = ArrayList<String>(ready.size)
            var bytes = 0L
            for (name in ready) {
                out.add(name)
                claimed.add(name)
                bytes += store.sizeOf(name)
                if (bytes >= maxBytes) break // Bound the batch; next claim takes the rest.
            }
            return out
        }
    }

    /**
     * Build the wire payload for the given segments: concatenate their entries (sequence order)
     * into ONE journal stream, zstd-compress it once, and report base64 + sha256 of the COMPRESSED
     * bytes + counts + first/last frame timestamps.
     *
     * Missing segments (already pruned) are skipped silently — a retry after a partial ack must not
     * hard-fail. Returns null when no segment survives.
     */
    fun buildRawPayload(segmentNames: List<String>): RawPayloadDto? {
        if (segmentNames.isEmpty()) return null
        val entries = ArrayList<JournalEntry>()
        for (name in segmentNames.sorted()) {
            val compressed = store.readSegment(name) ?: continue
            val journal = try {
                JournalCodec.decompress(compressed)
            } catch (_: Throwable) {
                continue // Corrupt segment: skip rather than poison the whole batch.
            }
            entries.addAll(JournalCodec.decodeJournal(journal))
        }
        if (entries.isEmpty()) return null

        val uncompressed = JournalCodec.encodeJournal(entries)
        val compressed = JournalCodec.compress(uncompressed)
        val sha256 = MessageDigest.getInstance("SHA-256").digest(compressed)
            .joinToString("") { "%02x".format(it) }
        return RawPayloadDto(
            codec = SyncContract.CODEC_ZSTD,
            journalVersion = SyncContract.JOURNAL_VERSION,
            frameCount = entries.size.toLong(),
            payloadB64 = Base64.getEncoder().encodeToString(compressed),
            payloadSha256 = sha256,
            uncompressedBytes = uncompressed.size.toLong(),
            firstFrameTs = entries.first().epochMs.toIsoInstant(),
            lastFrameTs = entries.last().epochMs.toIsoInstant(),
        )
    }

    /**
     * Delete segments — ONLY after the server acked a batch carrying them with raw_ack = true.
     * This is the server-authorized prune; nothing else may call it.
     */
    fun pruneSegments(segmentNames: List<String>): Int {
        var deleted = 0
        synchronized(lock) {
            segmentNames.forEach { name ->
                claimed.remove(name)
                if (store.deleteSegment(name)) deleted++
            }
        }
        return deleted
    }

    /** Return claimed segments to the ready pool (batch permanently failed; frames will re-ship). */
    fun releaseClaims(segmentNames: List<String>) {
        synchronized(lock) { claimed.removeAll(segmentNames.toSet()) }
    }

    // --- observability (no frame contents, no tokens — sizes and counts only) ---

    fun readySegmentCount(): Int = synchronized(lock) { store.listSegments().count { it !in claimed } }

    fun readyBytes(): Long = synchronized(lock) {
        store.listSegments().filter { it !in claimed }.sumOf { store.sizeOf(it) }
    }

    fun bufferedFrameCount(): Long = synchronized(lock) { bufferedFrames }

    fun capturedFrameCount(): Long = writtenFrames.get()

    fun droppedSegmentCount(): Long = droppedFrames.get()

    private fun Long.toIsoInstant(): String = Instant.ofEpochMilli(this).toString()
}
