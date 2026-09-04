package com.noop.sync

import android.content.Context
import com.noop.sync.api.SyncApiClient
import com.noop.sync.capture.RawJournalWriter
import com.noop.sync.pairing.DeviceTokenStore
import com.noop.sync.queue.FileSyncQueue
import com.noop.sync.worker.SyncEngine
import java.io.File

/*
 * Process-wide handle to the assembled sync stack (fork addition).
 *
 * The app's bridge builds the object graph once at startup (see SomatriqSyncBridge in :app) and
 * installs it here; [com.noop.sync.worker.SyncWorker] and any UI reach the same single instances.
 * Kept in the :sync module so the worker has no dependency on app wiring code.
 */

class SyncRuntime(
    val apiClient: SyncApiClient,
    val journal: RawJournalWriter,
    val queue: FileSyncQueue,
    val repository: SyncRepository,
    val watermark: SyncWatermark,
    val tokenStore: DeviceTokenStore,
    val diagnostics: SyncDiagnostics,
    val engine: SyncEngine,
) {
    companion object {
        @Volatile
        private var installed: SyncRuntime? = null

        fun install(runtime: SyncRuntime) {
            installed = runtime
        }

        fun get(context: Context): SyncRuntime? = installed

        /** Test hook — not used by production code paths. */
        fun reset() {
            installed = null
        }
    }
}

/** Standard on-disk layout under filesDir/somatriq: raw/ segments, queue/ entries, state files. */
object SyncPaths {
    fun root(context: Context): File = File(context.filesDir, "somatriq")
    fun rawDir(context: Context): File = File(root(context), "raw")
    fun queueDir(context: Context): File = File(root(context), "queue")
    fun watermarkFile(context: Context): File = File(root(context), "watermark.json")
}
