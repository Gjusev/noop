package com.noop.sync.capture

import com.noop.sync.SyncContract
import java.io.File
import java.io.IOException
import java.util.concurrent.atomic.AtomicLong

/*
 * Segment storage for the raw journal (fork addition).
 *
 * Segments are small self-contained zstd files named seg-<seq>.zst. Writes are atomic
 * (tmp-file + rename) so a crash mid-write never leaves a truncated segment masquerading as ready.
 * Pure java.io — no Android framework — so the whole journal stack is unit-testable on the JVM.
 */

/** Storage abstraction over the segment directory. Implementations must be safe for concurrent use. */
interface SegmentStore {

    /** A fresh, never-before-used segment name in sequence order. */
    fun nextSegmentName(): String

    /** Persist one compressed segment atomically. Throws [IOException] on failure. */
    fun writeSegment(name: String, compressed: ByteArray)

    /** Read a segment's compressed bytes, or null if it no longer exists. */
    fun readSegment(name: String): ByteArray?

    /** On-disk size of one segment in bytes, 0 when missing — cheap, never reads contents. */
    fun sizeOf(name: String): Long

    /** Delete a segment (only ever called after the server acked it). Returns true if deleted. */
    fun deleteSegment(name: String): Boolean

    /** All segment names currently on disk, in name (= sequence) order. */
    fun listSegments(): List<String>

    /** Total on-disk bytes of all segments. */
    fun totalBytes(): Long
}

/** Filesystem-backed store under [dir]; creates the directory on first use. */
class FileSegmentStore(private val dir: File) : SegmentStore {

    private val seq = AtomicLong(0)

    init {
        if (!dir.exists() && !dir.mkdirs()) {
            throw IOException("cannot create segment dir: $dir")
        }
        // Continue the sequence after any pre-existing segments so a restart never reuses a name.
        listSegments().lastOrNull()?.let { last ->
            last.removePrefix(SyncContract.SEGMENT_PREFIX).removeSuffix(SyncContract.SEGMENT_SUFFIX)
                .toLongOrNull()?.let { seq.set(it) }
        }
    }

    /** Monotonic, restart-safe next segment name. */
    override fun nextSegmentName(): String {
        val n = seq.incrementAndGet()
        return SyncContract.SEGMENT_PREFIX + n.toString().padStart(6, '0') + SyncContract.SEGMENT_SUFFIX
    }

    override fun sizeOf(name: String): Long = File(dir, name).takeIf { it.exists() }?.length() ?: 0L

    override fun writeSegment(name: String, compressed: ByteArray) {
        val target = File(dir, name)
        val tmp = File(dir, "$name.tmp")
        synchronized(this) {
            try {
                tmp.outputStream().use { it.write(compressed) }
                if (!tmp.renameTo(target)) {
                    // Rename within one directory should be atomic; fall back defensively.
                    tmp.copyTo(target, overwrite = true)
                    tmp.delete()
                }
            } finally {
                tmp.delete()
            }
        }
    }

    override fun readSegment(name: String): ByteArray? {
        val f = File(dir, name)
        if (!f.exists()) return null
        return synchronized(this) { f.readBytes() }
    }

    override fun deleteSegment(name: String): Boolean = synchronized(this) { File(dir, name).delete() }

    override fun listSegments(): List<String> =
        dir.listFiles { f -> f.isFile && f.name.startsWith(SyncContract.SEGMENT_PREFIX) && f.name.endsWith(SyncContract.SEGMENT_SUFFIX) }
            ?.map { it.name }
            ?.sorted()
            ?: emptyList()

    override fun totalBytes(): Long =
        dir.listFiles { f -> f.isFile && f.name.endsWith(SyncContract.SEGMENT_SUFFIX) }?.sumOf { it.length() } ?: 0L
}
