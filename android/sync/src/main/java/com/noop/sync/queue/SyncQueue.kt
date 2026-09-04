package com.noop.sync.queue

import com.noop.sync.SyncContract
import com.noop.sync.dto.DtoJson
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.io.File
import java.util.UUID

/*
 * Durable pending-batch registry (fork addition).
 *
 * DESIGN CHOICE — JSON files, not a second Room database: NOOP already owns one Room database
 * (WhoopDatabase, schema-locked to the Swift twin by the schema-oracle tests). A separate small
 * database in :sync would add a KSP/Room setup and a schema that no oracle guards, for a handful
 * of tiny rows. One JSON file per queue entry, written atomically (tmp + rename), is crash-safe,
 * trivially inspectable, and unit-testable on the plain JVM. The batch PAYLOAD itself never lives
 * here — observations are re-read from NOOP's Room DB by ts range and raw frames live in journal
 * segment files — so entries stay a few hundred bytes.
 *
 * The batch UUID is generated EXACTLY ONCE, at [enqueue] — it is the idempotency anchor the server
 * dedupes on, so a retried batch (same file, same batch_id) can never double-insert.
 */

enum class QueueEntryState { PENDING, IN_FLIGHT, ACKED, FAILED_PERMANENT }

/**
 * Which ingest endpoint an entry ships to. "hr" is the original family (HR observations + optional
 * raw journal, schema "2"); the others are the single-family schema-"1" endpoints. Defaulted so
 * queue files written before the families existed decode as HR unchanged.
 */
enum class QueueFamily {
    @SerialName("hr") HR,
    @SerialName("daily") DAILY,
    @SerialName("sleep") SLEEP,
    @SerialName("rr") RR,
}

@Serializable
data class QueueEntry(
    /** Generated once at enqueue; never regenerated on retry. Server idempotency key. */
    @SerialName("batch_id") val batchId: String,
    @SerialName("created_at_ms") val createdAtMs: Long,
    @SerialName("attempt_count") val attemptCount: Int = 0,
    @SerialName("next_attempt_at_ms") val nextAttemptAtMs: Long = 0,
    @SerialName("state") val state: QueueEntryState = QueueEntryState.PENDING,
    @SerialName("family") val family: QueueFamily = QueueFamily.HR,
    /** Inclusive ts window (unix seconds) of observations this batch covers (HR/sleep/rr). */
    @SerialName("obs_from_ts") val obsFromTsInclusive: Long,
    @SerialName("obs_to_ts") val obsToTsInclusive: Long,
    /** Journal segments claimed by this batch; pruned only on raw_ack. */
    @SerialName("segments") val segments: List<String> = emptyList(),
    /** Inclusive day window (yyyy-MM-dd) when [family] is DAILY; null otherwise. */
    @SerialName("day_from") val dayFromInclusive: String? = null,
    @SerialName("day_to") val dayToInclusive: String? = null,
    @SerialName("acked_at_ms") val ackedAtMs: Long? = null,
)

class FileSyncQueue(private val dir: File) {

    init {
        if (!dir.exists()) dir.mkdirs()
    }

    private fun fileFor(batchId: String) = File(dir, "$batchId.json")

    @Synchronized
    fun enqueue(obsFromTsInclusive: Long, obsToTsInclusive: Long, segments: List<String>, nowMs: Long): QueueEntry {
        val entry = QueueEntry(
            batchId = UUID.randomUUID().toString(),
            createdAtMs = nowMs,
            family = QueueFamily.HR,
            obsFromTsInclusive = obsFromTsInclusive,
            obsToTsInclusive = obsToTsInclusive,
            segments = segments,
        )
        write(entry)
        return entry
    }

    /**
     * Enqueue a single-family batch (daily/sleep/rr). The ts window carries sleep/rr bounds
     * (INCLUSIVE lower bound — boundary re-read, server dedupe); the day window carries daily's.
     */
    @Synchronized
    fun enqueueFamily(
        family: QueueFamily,
        obsFromTsInclusive: Long,
        obsToTsInclusive: Long,
        dayFromInclusive: String?,
        dayToInclusive: String?,
        nowMs: Long,
    ): QueueEntry {
        require(family != QueueFamily.HR) { "use enqueue() for the HR family" }
        val entry = QueueEntry(
            batchId = UUID.randomUUID().toString(),
            createdAtMs = nowMs,
            family = family,
            obsFromTsInclusive = obsFromTsInclusive,
            obsToTsInclusive = obsToTsInclusive,
            dayFromInclusive = dayFromInclusive,
            dayToInclusive = dayToInclusive,
        )
        write(entry)
        return entry
    }

    /** Oldest due PENDING entry (nextAttemptAtMs in the past), or null. */
    @Synchronized
    fun nextDue(nowMs: Long): QueueEntry? =
        loadAll()
            .filter { it.state == QueueEntryState.PENDING && it.nextAttemptAtMs <= nowMs }
            .minByOrNull { it.createdAtMs }

    @Synchronized
    fun update(entry: QueueEntry): QueueEntry {
        write(entry)
        return entry
    }

    fun markInFlight(entry: QueueEntry, nowMs: Long): QueueEntry =
        update(entry.copy(state = QueueEntryState.IN_FLIGHT, attemptCount = entry.attemptCount + 1))

    fun markAcked(entry: QueueEntry, nowMs: Long): QueueEntry =
        update(entry.copy(state = QueueEntryState.ACKED, ackedAtMs = nowMs))

    fun markFailedPermanent(entry: QueueEntry): QueueEntry =
        update(entry.copy(state = QueueEntryState.FAILED_PERMANENT))

    /** Exponential backoff: 30s, 2m, 8m, 32m … capped at 2h. */
    fun scheduleRetry(entry: QueueEntry, nowMs: Long): QueueEntry {
        val delay = retryDelayMs(entry.attemptCount)
        return update(
            entry.copy(
                state = QueueEntryState.PENDING,
                nextAttemptAtMs = nowMs + delay,
            ),
        )
    }

    /**
     * Return every IN_FLIGHT entry to PENDING (attempt count kept). Called at worker start: an
     * entry left IN_FLIGHT means a previous worker run died mid-send; the same batch_id retry is
     * idempotent server-side.
     */
    @Synchronized
    fun requeueStaleInFlight(): Int {
        var n = 0
        loadAll().filter { it.state == QueueEntryState.IN_FLIGHT }.forEach {
            update(it.copy(state = QueueEntryState.PENDING))
            n++
        }
        return n
    }

    @Synchronized
    fun loadAll(): List<QueueEntry> =
        dir.listFiles { f -> f.isFile && f.name.endsWith(".json") }
            ?.mapNotNull { f -> runCatching { read(f) }.getOrNull() }
            ?.sortedBy { it.createdAtMs }
            ?: emptyList()

    @Synchronized
    fun pendingCount(): Int = loadAll().count { it.state == QueueEntryState.PENDING }

    /** Housekeeping: drop acked entries older than [maxAgeMs]. Returns files removed. */
    @Synchronized
    fun pruneAcked(nowMs: Long, maxAgeMs: Long): Int {
        var n = 0
        loadAll().filter { it.state == QueueEntryState.ACKED && (it.ackedAtMs ?: 0) < nowMs - maxAgeMs }
            .forEach { if (fileFor(it.batchId).delete()) n++ }
        return n
    }

    private fun write(entry: QueueEntry) {
        val target = fileFor(entry.batchId)
        val tmp = File(dir, target.name + ".tmp")
        try {
            tmp.writeText(DtoJson.json.encodeToString(QueueEntry.serializer(), entry))
            if (!tmp.renameTo(target)) {
                tmp.copyTo(target, overwrite = true)
                tmp.delete()
            }
        } finally {
            tmp.delete()
        }
    }

    private fun read(f: File): QueueEntry = DtoJson.json.decodeFromString(QueueEntry.serializer(), f.readText())

    companion object {
        fun retryDelayMs(attemptCount: Int): Long {
            if (attemptCount <= 1) return SyncContract.RETRY_BACKOFF_BASE_MS
            val delay = SyncContract.RETRY_BACKOFF_BASE_MS * (1L shl minOf(attemptCount - 1, 8))
            return delay.coerceAtMost(SyncContract.RETRY_BACKOFF_MAX_MS)
        }
    }
}
