package com.noop.somatriq

import android.util.Log
import com.noop.BuildConfig
import com.noop.NoopApplication
import com.noop.data.WhoopDatabase
import com.noop.sync.DailyMetricObservation
import com.noop.sync.HrObservation
import com.noop.sync.ObservationSource
import com.noop.sync.RrIntervalObservation
import com.noop.sync.SleepSessionObservation
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
 * Streams shipped: measured strap HR ([com.noop.data.WhoopDao.rawHrSamples], NOT the v26 PPG-derived
 * union — Somatriq wants sensor observations; derived estimates would double-represent the same
 * second), dailyMetric, sleepSession, and rrInterval — every read goes through an EXISTING
 * WhoopDao query, so this fork adds no NOOP query/table/DAO surface.
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

    /**
     * The device's local yyyy-MM-dd for today — the SAME local-day keying NOOP's #277 dailyMetric
     * re-bucketing uses (day = dayString(ts, tzOffsetSec) with the default TimeZone; LocalDate.now()
     * reads the same default zone). Today's row keeps mutating until the day is over, so the sync
     * layer holds it back and ships it only once the day is complete.
     */
    override suspend fun todayDay(): String? = runCatching { java.time.LocalDate.now().toString() }
        .getOrDefault(null)

    override suspend fun dailyMetrics(fromDayInclusive: String, limit: Int): List<DailyMetricObservation> =
        runCatching {
            val deviceId = app.activeDeviceId
            if (deviceId.isEmpty()) {
                emptyList()
            } else {
                val today = todayDay() ?: return emptyList()
                WhoopDatabase.get(app).whoopDao()
                    .dailyMetricsRange(deviceId, from = fromDayInclusive, to = today)
                    .filter { it.day >= fromDayInclusive && it.day < today }
                    .take(limit)
                    .map { it.toObservation() }
            }
        }.getOrDefault(emptyList())

    override suspend fun sleepSessions(fromStartTsInclusive: Long, limit: Int): List<SleepSessionObservation> =
        runCatching {
            val deviceId = app.activeDeviceId
            if (deviceId.isEmpty()) {
                emptyList()
            } else {
                // Only FINISHED sessions ship (endTs in the past): an open night is still being
                // staged and its stagesJSON would arrive half-derived. The DAO limit is padded so
                // filtering the open session never starves a full window (see SYNC-NOTES §3.6).
                val now = System.currentTimeMillis() / 1000
                WhoopDatabase.get(app).whoopDao()
                    .sleepSessions(deviceId, from = fromStartTsInclusive, to = now, limit = limit + 8)
                    .filter { it.startTs >= fromStartTsInclusive && it.endTs <= now }
                    .take(limit)
                    .map {
                        SleepSessionObservation(
                            sourceRecordId = "sleep:$deviceId:${it.startTs}",
                            startTsEpochSeconds = it.startTs,
                            endTsEpochSeconds = it.endTs,
                            efficiency = it.efficiency,
                            restingHr = it.restingHr,
                            avgHrv = it.avgHrv,
                            userEdited = it.userEdited,
                            stagesJSON = it.stagesJSON,
                        )
                    }
            }
        }.getOrDefault(emptyList())

    override suspend fun rrIntervals(fromTsInclusive: Long, limit: Int): List<RrIntervalObservation> =
        runCatching {
            val deviceId = app.activeDeviceId
            if (deviceId.isEmpty()) {
                emptyList()
            } else {
                // The DAO read applies NOOP's own R-R filters (excludes the redundant SPO2_IBI
                // channel and future-stamped tsSuspect rows) — sync sees exactly what local
                // scoring sees, never a duplicated or corrupt beat.
                WhoopDatabase.get(app).whoopDao()
                    .rrIntervals(deviceId, from = fromTsInclusive, to = Long.MAX_VALUE, limit = limit)
                    .map {
                        RrIntervalObservation(
                            sourceRecordId = "rr:$deviceId:${it.ts}:${it.rrMs}:${it.seq}",
                            tsEpochSeconds = it.ts,
                            rrMs = it.rrMs,
                            seq = it.seq,
                        )
                    }
            }
        }.getOrDefault(emptyList())
}

/** Field-for-field copy onto the sync module's mirror type — the mapping table lives in :sync. */
private fun com.noop.data.DailyMetric.toObservation() = DailyMetricObservation(
    day = day,
    totalSleepMin = totalSleepMin,
    efficiency = efficiency,
    deepMin = deepMin,
    remMin = remMin,
    lightMin = lightMin,
    disturbances = disturbances,
    restingHr = restingHr,
    avgHrv = avgHrv,
    recovery = recovery,
    strain = strain,
    exerciseCount = exerciseCount,
    spo2Pct = spo2Pct,
    skinTempDevC = skinTempDevC,
    respRateBpm = respRateBpm,
    steps = steps,
    activeKcalEst = activeKcalEst,
    avgSdnn = avgSdnn,
    skinTempC = skinTempC,
)
