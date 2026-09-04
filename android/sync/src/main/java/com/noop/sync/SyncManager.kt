package com.noop.sync

import android.content.Context
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.OutOfQuotaPolicy
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import com.noop.sync.pairing.PairingManager
import com.noop.sync.pairing.PairingOutcome
import com.noop.sync.worker.SyncWorker
import java.util.concurrent.TimeUnit

/*
 * Sync facade (fork addition): pairing, expedited syncs, the periodic schedule, and the status
 * Flow for the UI.
 *
 * CADENCE DEVIATION (documented in docs/somatriq/SYNC-NOTES.md): the Somatriq spec targets a
 * ~5-minute sync cadence, but WorkManager's periodic floor is 15 minutes. The periodic schedule
 * runs at 15 min and the gap is closed opportunistically: expedited one-time syncs fire on app
 * foreground and on completed-sleep/workout events (the host app calls [onForegroundEvent]).
 *
 * BATTERY: every request — periodic and expedited — requires NetworkType.CONNECTED and
 * requiresBatteryNotLow, so sync never runs on a nearly-dead phone.
 */

class SyncManager(
    private val context: Context,
    private val runtime: SyncRuntime,
    private val pairingManager: PairingManager,
) {
    val status: kotlinx.coroutines.flow.StateFlow<SyncState> = runtime.diagnostics.state

    /** Exchange a pairing code for a device token (see [PairingManager.confirm]). */
    fun pair(pairingCode: String, deviceName: String): PairingOutcome =
        pairingManager.confirm(pairingCode, deviceName)

    fun unpair() {
        pairingManager.unpair()
        runtime.diagnostics.markAwaitingPairing()
    }

    /** Schedule the 15-minute periodic drain (idempotent; re-enrolls on every app start). */
    fun schedule() {
        WorkManager.getInstance(context).enqueueUniquePeriodicWork(
            PERIODIC_WORK_NAME,
            ExistingPeriodicWorkPolicy.UPDATE,
            PeriodicWorkRequestBuilder<SyncWorker>(
                SyncContract.PERIODIC_SYNC_MINUTES, TimeUnit.MINUTES,
            )
                .setConstraints(syncConstraints())
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .build(),
        )
    }

    /** One expedited drain — foreground, post-workout/sleep, post-pairing, or user pull. */
    fun forceSyncNow() {
        WorkManager.getInstance(context).enqueueUniqueWork(
            ONE_SHOT_WORK_NAME,
            ExistingWorkPolicy.REPLACE,
            OneTimeWorkRequestBuilder<SyncWorker>()
                .setConstraints(syncConstraints())
                .setExpedited(OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
                .build(),
        )
    }

    /** Host app calls this on foreground / completed-session events to beat the 15-min floor. */
    fun onForegroundEvent() {
        if (pairingManager.isPaired()) forceSyncNow()
    }

    private fun syncConstraints(): Constraints = Constraints.Builder()
        .setRequiredNetworkType(NetworkType.CONNECTED)
        .setRequiresBatteryNotLow(true)
        .build()

    companion object {
        const val PERIODIC_WORK_NAME = "somatriq-sync-periodic"
        const val ONE_SHOT_WORK_NAME = "somatriq-sync-now"
    }
}
