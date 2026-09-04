package com.noop.sync.queue

import com.noop.sync.SyncContract
import com.noop.sync.SyncWatermark
import java.io.File
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

/*
 * Durable queue + watermark tests (fork addition): batch_id minted once and stable across
 * retries; entries survive a process restart (new instance over the same dir); retry backoff is
 * exponential and capped; the watermark never moves backwards.
 */
class FileSyncQueueTest {

    @get:Rule
    val tmp = TemporaryFolder()

    private lateinit var dir: File
    private lateinit var queue: FileSyncQueue

    @Before
    fun setUp() {
        dir = tmp.newFolder("queue")
        queue = FileSyncQueue(dir)
    }

    @Test
    fun `enqueue mints a batch id once and it is stable across retries`() {
        val e = queue.enqueue(0, 100, listOf("seg-000001.zst"), nowMs = 1_000)
        assertNotNull(e.batchId)
        assertEquals(QueueEntryState.PENDING, e.state)

        val inFlight = queue.markInFlight(e, nowMs = 2_000)
        assertEquals(e.batchId, inFlight.batchId)
        assertEquals(1, inFlight.attemptCount)

        val retried = queue.scheduleRetry(inFlight, nowMs = 3_000)
        assertEquals(e.batchId, retried.batchId) // SAME id — the idempotency anchor
        assertEquals(QueueEntryState.PENDING, retried.state)
        assertEquals(3_000 + FileSyncQueue.retryDelayMs(1), retried.nextAttemptAtMs)
    }

    @Test
    fun `nextDue respects nextAttemptAt and FIFO order`() {
        val a = queue.enqueue(0, 10, emptyList(), nowMs = 1_000)
        val b = queue.enqueue(11, 20, emptyList(), nowMs = 1_100)
        assertEquals(a.batchId, queue.nextDue(1_500)!!.batchId)

        val bumped = queue.scheduleRetry(a, nowMs = 1_500)
        assertEquals(b.batchId, queue.nextDue(1_600)!!.batchId) // a not due yet
        assertEquals(a.batchId, queue.nextDue(bumped.nextAttemptAtMs)!!.batchId)
        assertNull(queue.nextDue(Long.MIN_VALUE)) // nothing due before time began
    }

    @Test
    fun `in-flight entries from a dead run are requeued with attempt count kept`() {
        val e = queue.enqueue(0, 10, emptyList(), nowMs = 1_000)
        queue.markInFlight(e, nowMs = 2_000)
        assertEquals(1, queue.requeueStaleInFlight())
        val requeued = queue.loadAll().single()
        assertEquals(QueueEntryState.PENDING, requeued.state)
        assertEquals(1, requeued.attemptCount) // the dead attempt stays counted
    }

    @Test
    fun `entries survive a restart - new instance reads the same files`() {
        val e = queue.enqueue(0, 42, listOf("seg-000001.zst", "seg-000002.zst"), nowMs = 1_000)
        queue.markAcked(e, nowMs = 2_000)

        val reopened = FileSyncQueue(dir)
        val loaded = reopened.loadAll().single()
        assertEquals(e.batchId, loaded.batchId)
        assertEquals(QueueEntryState.ACKED, loaded.state)
        assertEquals(listOf("seg-000001.zst", "seg-000002.zst"), loaded.segments)
    }

    @Test
    fun `backoff is exponential and capped at the contract maximum`() {
        assertEquals(SyncContract.RETRY_BACKOFF_BASE_MS, FileSyncQueue.retryDelayMs(1))
        assertTrue(FileSyncQueue.retryDelayMs(2) > FileSyncQueue.retryDelayMs(1))
        assertTrue(FileSyncQueue.retryDelayMs(5) > FileSyncQueue.retryDelayMs(4))
        for (attempt in 1..20) {
            assertTrue(FileSyncQueue.retryDelayMs(attempt) <= SyncContract.RETRY_BACKOFF_MAX_MS)
        }
        assertEquals(SyncContract.RETRY_BACKOFF_MAX_MS, FileSyncQueue.retryDelayMs(50))
    }

    @Test
    fun `pruneAcked removes only old acked entries`() {
        val e1 = queue.enqueue(0, 10, emptyList(), nowMs = 1_000)
        val e2 = queue.enqueue(11, 20, emptyList(), nowMs = 1_100)
        queue.markAcked(e1, nowMs = 2_000)
        queue.markAcked(e2, nowMs = 95_000) // acked 5s ago — still fresh

        assertEquals(1, queue.pruneAcked(nowMs = 100_000, maxAgeMs = 10_000))
        assertEquals(listOf(e2.batchId), queue.loadAll().map { it.batchId })
    }

    @Test
    fun `watermark is monotonic and persists`() {
        val file = File(tmp.root, "watermark.json")
        val wm = SyncWatermark(file)
        assertEquals(0L, wm.lastAckedHrTs())
        assertEquals(100L, wm.advanceHrTs(100))
        assertEquals(100L, wm.advanceHrTs(50)) // backwards is ignored
        assertEquals(150L, wm.advanceHrTs(150))

        val reopened = SyncWatermark(file)
        assertEquals(150L, reopened.lastAckedHrTs())
    }
}
