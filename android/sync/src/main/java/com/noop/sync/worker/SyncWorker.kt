package com.noop.sync.worker

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import com.noop.sync.SyncRuntime
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/*
 * WorkManager entry point (fork addition). Thin: the whole drain state machine is [SyncEngine],
 * which knows nothing of WorkManager and is JVM-unit-testable. This worker only adapts
 * cancellation and coroutine context.
 *
 * Cancellation semantics: CoroutineWorker cancellation surfaces as a CancellationException from
 * any suspension, or we observe it between batches via [isStopped]. Either way the in-flight
 * batch stays in the queue with its ORIGINAL batch_id — the next run re-sends it and the server
 * dedupes. Nothing is ever half-committed: ack effects (watermark, prune, queue state) happen
 * atomically per batch after the response.
 */

class SyncWorker(appContext: Context, params: WorkerParameters) :
    CoroutineWorker(appContext, params) {

    override suspend fun doWork(): Result = withContext(Dispatchers.IO) {
        val runtime = SyncRuntime.get(applicationContext)
            ?: return@withContext Result.failure() // not installed: nothing to sync
        val engine = runtime.engine
        return@withContext when (val r = engine.runPass(StopSignal { isStopped })) {
            is EnginePassResult.Drained,
            is EnginePassResult.MoreRemaining,
            -> Result.success()
            // Retryable failures already rescheduled themselves in the queue (nextAttemptAt +
            // periodic/expedited re-runs); no WorkManager retry needed on top.
            is EnginePassResult.RetryLater -> Result.success()
            // Halted needs user action (pairing, or a permanent rejection) — success keeps
            // WorkManager from hammering; the state Flow shows the real status.
            is EnginePassResult.Halted -> Result.success()
        }
    }
}
