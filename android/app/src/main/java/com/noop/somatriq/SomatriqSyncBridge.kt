package com.noop.somatriq

import android.util.Log
import com.noop.BuildConfig
import com.noop.NoopApplication
import com.noop.data.WhoopDatabase
import com.noop.sync.HrObservation
import com.noop.sync.ObservationSource
import com.noop.sync.SyncContract
import com.noop.sync.SyncDiagnostics
import com.noop.sync.SyncManager
import com.noop.sync.SyncPaths
import com.noop.sync.SyncRepository
import com.noop.sync.SyncRuntime
import com.noop.sync.SyncWatermark
import com.noop.sync.api.SyncApiClient
import com.noop.sync.api.SyncConfig
import com.noop.sync.capture.FileSegmentStore
import com.noop.sync.capture.RawCapturePoint
import com.noop.sync.capture.RawJournalWriter
import com.noop.sync.pairing.DeviceTokenStore
import com.noop.sync.pairing.EncryptedDeviceTokenStore
import com.noop.sync.pairing.PairingManager
import com.noop.sync.queue.FileSyncQueue
import com.noop.sync.worker.SyncEngine
import java.util.concurrent.Executors

/*
 * Somatriq wiring (fork addition) — the ONE place the host app meets the sync module.
 *
 * Built at app start from NoopApplication.onCreate. Fully guarded: any failure here is logged and
 * swallowed, so a sync problem can NEVER block NOOP startup or the BLE stack. The wiring is
 * additive: nothing in NOOP's data or protocol packages imports this.
 *
 * What runs unpaired vs paired (spec: local-first, network opt-in):
 *   - RAW CAPTURE runs always once installed — pure local disk under the app's private dir,
 *     bounded by rotation and pruned only on the server's raw_ack. No bytes leave the phone until
 *     a batch ships.
 *   - NETWORK SYNC (WorkManager schedule) starts only when a device token exists, i.e. after the
 *     user explicitly paired with their Somatriq server. NOOP's "network is opt-in" stance holds.
 */

object SomatriqSyncBridge {

    private const val TAG = "SomatriqSync"

    @Volatile
    var manager: SyncManager? = null
        private set

    @Volatile
    private var journal: RawJournalWriter? = null

    fun install(app: NoopApplication) {
        try {
            val segmentIo = Executors.newSingleThreadExecutor { r ->
                Thread(r, "somatriq-raw-journal").apply { isDaemon = true }
            }

            val segmentStore = FileSegmentStore(SyncPaths.rawDir(app))
            val writer = RawJournalWriter(segmentStore, segmentIo)
            journal = writer

            val tokenStore: DeviceTokenStore = EncryptedDeviceTokenStore(app)
            val queue = FileSyncQueue(SyncPaths.queueDir(app))
            val repository = SyncRepository(WhoopObservationSource(app))
            val watermark = SyncWatermark(SyncPaths.watermarkFile(app))
            val diagnostics = SyncDiagnostics()
            val apiClient = SyncApiClient(SyncConfig(decoderVersion = decoderVersion()))
            val engine = SyncEngine(
                queue = queue,
                journal = writer,
                repository = repository,
                watermark = watermark,
                apiClient = apiClient,
                tokenStore = tokenStore,
                diagnostics = diagnostics,
            )
            val runtime = SyncRuntime(
                apiClient = apiClient,
                journal = writer,
                queue = queue,
                repository = repository,
                watermark = watermark,
                tokenStore = tokenStore,
                diagnostics = diagnostics,
                engine = engine,
            )
            SyncRuntime.install(runtime)

            val syncManager = SyncManager(app, runtime, PairingManager(apiClient, tokenStore))
            manager = syncManager

            // The pre-decoder tap: every complete frame the reassembler hands the decoder also
            // lands in the local journal (no-op until this line runs; never throws into BLE).
            RawCapturePoint.install { frame -> writer.capture(frame) }

            val paired = tokenStore.token() != null
            if (paired) syncManager.schedule() else diagnostics.markAwaitingPairing()
            Log.i(TAG, "somatriq sync installed (paired=$paired)")
        } catch (t: Throwable) {
            // Deliberately broad: NOOP must start and record wearable data regardless of sync.
            Log.w(TAG, "somatriq sync install failed; running without sync", t)
        }
    }

    /** e.g. "noop-android/11.1.1+somatriq" — the frozen decoder_version shape. */
    fun decoderVersion(): String =
        SyncContract.DECODER_VERSION_PREFIX + "/" + BuildConfig.VERSION_NAME + "+somatriq"

    /** Call when the app leaves the foreground: flush the in-memory frame buffer to a segment. */
    fun onAppBackground() {
        runCatching { journal?.flushNow() }
    }
}

/**
 * Reads NOOP's Room store for the sync engine. READ-ONLY by construction — [ObservationSource]
 * has no write method: the sync layer never touches NOOP's tables. (The `synced` column is unused
 * by NOOP — nothing ever writes it to 1 — so the sync watermark lives in the sync module's own
 * state file; see docs/somatriq/SYNC-NOTES.md.)
 *
 * Only `hrSample` ships in this milestone — measured strap HR via [com.noop.data.WhoopDao.rawHrSamples],
 * NOT the v26 PPG-derived union (Somatriq wants sensor observations; derived estimates would
 * double-represent the same second).
 */
private class WhoopObservationSource(private val app: NoopApplication) : ObservationSource {

    override suspend fun hrSamples(fromTsExclusive: Long, toTsInclusive: Long, limit: Int): List<HrObservation> =
        runCatching {
            val deviceId = app.activeDeviceId
            if (deviceId.isEmpty()) {
                emptyList()
            } else {
                WhoopDatabase.get(app).whoopDao()
                    .rawHrSamples(deviceId, fromTsExclusive + 1, toTsInclusive, limit)
                    .map {
                        HrObservation(
                            sourceRecordId = "hrSample:$deviceId:${it.ts}",
                            tsEpochSeconds = it.ts,
                            bpm = it.bpm,
                        )
                    }
            }
        }.getOrDefault(emptyList())

    /**
     * Informational only (the engine batches from real rows, not from this): may report the
     * PPG-derived latest ts because NOOP's latestHrSampleTs coalesces both tables.
     */
    override suspend fun maxHrTs(): Long? = runCatching {
        val deviceId = app.activeDeviceId
        if (deviceId.isEmpty()) null else WhoopDatabase.get(app).whoopDao().latestHrSampleTs(deviceId)
    }.getOrDefault(null)
}
