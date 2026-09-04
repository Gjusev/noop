package com.noop.sync.capture

import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdException
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

/*
 * Raw Journal v1 codec (fork addition) — pure JVM, no Android framework.
 *
 * Journal format (frozen):
 *   journal   := entry*
 *   entry     := u32_be length N | u64_be epoch_ms | N bytes frame
 * The byte stream is zstd-compressed as a whole; `N` counts ONLY the frame bytes (never the header).
 * Multi-frame batches concatenate the entries of several segments in sequence order — the format
 * is a plain concatenation, so segment boundaries are invisible on the wire.
 */

/** One decoded journal entry: the capture clock reading plus the raw frame bytes. */
data class JournalEntry(val epochMs: Long, val frame: ByteArray) {
    override fun equals(other: Any?): Boolean =
        other is JournalEntry && other.epochMs == epochMs && other.frame.contentEquals(frame)

    override fun hashCode(): Int = 31 * epochMs.hashCode() + frame.contentHashCode()
}

object JournalCodec {

    /** Header size of one entry: u32 length + u64 epoch. */
    const val ENTRY_HEADER_BYTES: Int = 4 + 8

    /** Append one entry to [out] in journal order (big-endian). */
    fun encodeEntry(out: ByteArrayOutputStream, epochMs: Long, frame: ByteArray) {
        val header = ByteBuffer.allocate(ENTRY_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN)
        header.putInt(frame.size) // N excludes the header itself.
        header.putLong(epochMs)
        out.write(header.array())
        out.write(frame)
    }

    /** Encode a full uncompressed journal stream from entries. */
    fun encodeJournal(entries: List<JournalEntry>): ByteArray {
        val out = ByteArrayOutputStream(entries.sumOf { ENTRY_HEADER_BYTES + it.frame.size })
        entries.forEach { encodeEntry(out, it.epochMs, it.frame) }
        return out.toByteArray()
    }

    /** Split an uncompressed journal stream back into entries; throws on truncation or bad framing. */
    fun decodeJournal(journal: ByteArray): List<JournalEntry> {
        val entries = ArrayList<JournalEntry>()
        var pos = 0
        while (pos < journal.size) {
            require(journal.size - pos >= ENTRY_HEADER_BYTES) { "truncated entry header at $pos" }
            val buf = ByteBuffer.wrap(journal, pos, ENTRY_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN)
            val n = buf.int
            val epoch = buf.long
            require(n >= 0 && journal.size - pos - ENTRY_HEADER_BYTES >= n) { "truncated frame at $pos (len $n)" }
            val frame = journal.copyOfRange(pos + ENTRY_HEADER_BYTES, pos + ENTRY_HEADER_BYTES + n)
            entries.add(JournalEntry(epoch, frame))
            pos += ENTRY_HEADER_BYTES + n
        }
        return entries
    }

    fun compress(uncompressed: ByteArray): ByteArray = Zstd.compress(uncompressed, 3)

    fun decompress(compressed: ByteArray): ByteArray {
        // Our own compress() writes the content size into the frame, so the exact path is the norm.
        val size = Zstd.getFrameContentSize(compressed)
        if (size in 1..(Int.MAX_VALUE - 8).toLong()) {
            return Zstd.decompress(compressed, size.toInt())
        }
        // Defensive fallback for foreign frames with no content size: grow-and-retry.
        var capacity = 1 shl 20
        while (capacity <= MAX_DECOMPRESS_HINT) {
            try {
                return Zstd.decompress(compressed, capacity)
            } catch (_: ZstdException) {
                capacity = capacity shl 1
            }
        }
        throw IllegalStateException("journal frame exceeds the ${MAX_DECOMPRESS_HINT shr 20} MiB decompression ceiling")
    }

    private const val MAX_DECOMPRESS_HINT: Int = 256 * 1024 * 1024
}
