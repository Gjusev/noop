package com.noop.sync.worker

import com.noop.sync.SyncContract
import com.noop.sync.SyncDiagnostics
import com.noop.sync.SyncRepository
import com.noop.sync.SyncWatermark
import com.noop.sync.api.SyncApiClient
import com.noop.sync.api.SyncCallResult
import com.noop.sync.capture.RawJournalWriter
import com.noop.sync.dto.DailyObservationsRequestDto
import com.noop.sync.dto.IngestAckDto
import com.noop.sync.dto.IngestBatchRequestDto
import com.noop.sync.dto.RrIntervalsRequestDto
import com.noop.sync.dto.SleepSessionsRequestDto
import com.noop.sync.dto.Validation
import com.noop.sync.dto.Validators
import com.noop.sync.pairing.DeviceTokenStore
import com.noop.sync.queue.FileSyncQueue
import com.noop.sync.queue.QueueEntry
import com.noop.sync.queue.QueueFamily

/*
 * The sync drain loop (fork addition) — plain Kotlin, no WorkManager types, so the whole
 * state machine is unit-testable and reusable from any runner.
 *
 * INVARIANTS (Somatriq spec §34-39):
 *  - Local-first: a failing or absent network NEVER loses or blocks local capture; this engine
 *    only ever READS NOOP data (via ObservationSource) and deletes raw segments it owns.
 *  - Idempotent: the batch_id is minted once at enqueue and reused for every retry of that batch.
 *  - Raw prune is server-authorized: segments are deleted ONLY when an ack said raw_ack = true
 *    for a batch carrying them. An observations-only ack (raw_ack = false) advances the watermark
 *    but never prunes frames — they are released and re-attach to a later batch.
 *  - Cancel-safe: [StopSignal] is consulted between batches; an entry parked IN_FLIGHT by a killed
 *    run is returned to PENDING at the next pass and re-sent under the SAME batch_id.
 *  - No skipped rows: each batch's window is bounded to what fits in the family's row cap, so
 *    acking the window advances the watermark over exactly the rows that shipped (R-R re-reads its
 *    boundary ts inclusively — see SyncRepository.rrWindow).
 *
 * FAMILIES drain in a fixed order per pass — daily-observations, then sleep-sessions, then
 * rr-intervals, then the original HR+raw batches — because cheap, small, high-value data should
 * never queue behind a multi-request R-R backlog. Family watermarks advance ONLY on an accepted
 * ack (or an explicit skip — see [parkFamilyEntry]).
 */

/** Injectable "should I stop?" — WorkManager supplies isStopped; tests flip a flag. */
fun interface StopSignal {
    fun isStopped(): Boolean

    companion object {
        val NEVER = StopSignal { false }
    }
}

sealed class EnginePassResult {
    /** Everything shippable was shipped and acked; nothing left. */
    object Drained : EnginePassResult()

    /** Progress made, but more data remains (stopped mid-drain, batch bound, or pass bound). */
    data class MoreRemaining(val batchesAcked: Int) : EnginePassResult()

    /** Retryable failure; queue entries already carry their nextAttemptAt. */
    data class RetryLater(val code: String, val message: String, val batchesAcked: Int) : EnginePassResult()

    /** Terminal condition (credentials/permanent HR-batch failure/local bug); sync pauses until user action. */
    data class Halted(val code: String, val message: String) : EnginePassResult()
}

class SyncEngine(
    private val queue: FileSyncQueue,
    private val journal: RawJournalWriter,
    private val repository: SyncRepository,
    private val watermark: SyncWatermark,
    private val apiClient: SyncApiClient,
    private val tokenStore: DeviceTokenStore,
    private val diagnostics: SyncDiagnostics,
    private val clock: () -> Long = System::currentTimeMillis,
) {

    /** One drain pass. Returns the terminal outcome; safe to call repeatedly. */
    suspend fun runPass(isStopped: StopSignal = StopSignal.NEVER): EnginePassResult {
        val token = tokenStore.token()
        if (token == null) {
            diagnostics.markAwaitingPairing()
            return EnginePassResult.Halted("NOT_PAIRED", "no device token — pair first")
        }

        queue.requeueStaleInFlight()

        var batchesAcked = 0
        var batches = 0
        loop@ while (true) {
            if (isStopped.isStopped()) return EnginePassResult.MoreRemaining(batchesAcked)
            if (++batches > MAX_BATCHES_PER_PASS) return EnginePassResult.MoreRemaining(batchesAcked)

            val entry = queue.nextDue(clock()) ?: enqueueNextWindow() ?: break@loop

            diagnostics.markSyncing(queue.pendingCount())
            val inFlight = queue.markInFlight(entry, clock())

            when (val outcome = sendBatch(token, inFlight)) {
                is BatchOutcome.Acked -> batchesAcked++
                is BatchOutcome.Parked -> Unit // family batch skipped permanently; drain continues
                is BatchOutcome.RetryLater ->
                    return EnginePassResult.RetryLater(outcome.code, outcome.message, batchesAcked)
                is BatchOutcome.Halted ->
                    return EnginePassResult.Halted(outcome.code, outcome.message)
            }
        }
        return if (queue.pendingCount() > 0) EnginePassResult.MoreRemaining(batchesAcked)
        else EnginePassResult.Drained
    }

    // --- batch construction ---

    /**
     * Mint the next batch from whatever is new, families in fixed order: daily → sleep → rr → hr.
     * The first family with data wins; the loop comes back for the others on the next iteration
     * (bounded by MAX_BATCHES_PER_PASS, so an R-R backlog interleaves rather than monopolizes).
     */
    private suspend fun enqueueNextWindow(): QueueEntry? =
        enqueueDailyWindow() ?: enqueueSleepWindow() ?: enqueueRrWindow() ?: enqueueHrWindow()

    private suspend fun enqueueHrWindow(): QueueEntry? {
        val wm = watermark.lastAckedHrTs()
        val windowEnd = repository.nextWindowEnd(wm, SyncContract.MAX_RECORDS_PER_BATCH)
        val segments = journal.claimReadySegments()
        diagnostics.onSegmentsReady(journal.readySegmentCount())

        if (windowEnd == null && segments.isEmpty()) {
            journal.releaseClaims(segments) // claimed nothing; nothing to ship
            return null
        }
        val entry = queue.enqueue(
            obsFromTsInclusive = wm + 1,
            obsToTsInclusive = windowEnd ?: wm, // raw-only batch: empty observation window
            segments = segments,
            nowMs = clock(),
        )
        diagnostics.logEvent("enqueue family=hr batch=${entry.batchId.take(8)} obs≤${entry.obsToTsInclusive} segments=${segments.size}")
        return entry
    }

    private suspend fun enqueueDailyWindow(): QueueEntry? {
        val wm = watermark.lastAckedDailyDay()
        val window = repository.dailyWindow(wm, SyncContract.DAILY_MAX_ROWS_PER_BATCH) ?: return null
        val entry = queue.enqueueFamily(
            family = QueueFamily.DAILY,
            obsFromTsInclusive = 0,
            obsToTsInclusive = 0,
            dayFromInclusive = wm,
            dayToInclusive = window.second,
            nowMs = clock(),
        )
        diagnostics.logEvent("enqueue family=daily batch=${entry.batchId.take(8)} days≤${window.second} rows=${window.first.size}")
        return entry
    }

    private suspend fun enqueueSleepWindow(): QueueEntry? {
        val wm = watermark.lastAckedSleepTs()
        val window = repository.sleepWindow(wm, SyncContract.SLEEP_MAX_SESSIONS_PER_BATCH) ?: return null
        val entry = queue.enqueueFamily(
            family = QueueFamily.SLEEP,
            obsFromTsInclusive = wm,
            obsToTsInclusive = window.second,
            dayFromInclusive = null,
            dayToInclusive = null,
            nowMs = clock(),
        )
        diagnostics.logEvent("enqueue family=sleep batch=${entry.batchId.take(8)} startTs≤${window.second} sessions=${window.first.size}")
        return entry
    }

    private suspend fun enqueueRrWindow(): QueueEntry? {
        val wm = watermark.lastAckedRrTs()
        val window = repository.rrWindow(wm, SyncContract.RR_MAX_RECORDS_PER_BATCH) ?: return null
        val entry = queue.enqueueFamily(
            family = QueueFamily.RR,
            obsFromTsInclusive = wm,
            obsToTsInclusive = window.second,
            dayFromInclusive = null,
            dayToInclusive = null,
            nowMs = clock(),
        )
        diagnostics.logEvent("enqueue family=rr batch=${entry.batchId.take(8)} ts≤${window.second} records=${window.first.size}")
        return entry
    }

    // --- sending ---

    private suspend fun sendBatch(token: String, entry: QueueEntry): BatchOutcome =
        if (entry.family == QueueFamily.HR) sendHrBatch(token, entry) else sendFamilyBatch(token, entry)

    private suspend fun sendHrBatch(token: String, entry: QueueEntry): BatchOutcome {
        val records = repository.records(
            fromTsExclusive = entry.obsFromTsInclusive - 1,
            toTsInclusive = entry.obsToTsInclusive,
            limit = SyncContract.MAX_RECORDS_PER_BATCH,
        )
        val raw = if (entry.segments.isNotEmpty()) journal.buildRawPayload(entry.segments) else null

        if (records.isEmpty() && raw == null) {
            // Nothing survives to send (rows pruned locally, segments already acked-and-pruned).
            // Consume the window so the queue doesn't spin on it.
            watermark.advanceHrTs(entry.obsToTsInclusive)
            journal.releaseClaims(entry.segments)
            queue.markAcked(entry, clock())
            return BatchOutcome.Acked(0)
        }

        val request = IngestBatchRequestDto(
            batchId = entry.batchId,
            schemaVersion = SyncContract.SCHEMA_VERSION,
            decoderVersion = apiClient.config.decoderVersion,
            records = records,
            raw = raw,
        )
        when (val v = Validators.validateIngestBatch(request)) {
            is Validation.Invalid -> {
                // Our bug or corrupt local state: park it permanently, keep the frames.
                journal.releaseClaims(entry.segments)
                queue.markFailedPermanent(entry)
                diagnostics.logEvent("permanent batch=${entry.batchId.take(8)} local-validation: ${v.reason}")
                diagnostics.onFailure("LOCAL_VALIDATION", v.reason, queue.pendingCount())
                return BatchOutcome.Halted("LOCAL_VALIDATION", v.reason)
            }
            Validation.Valid -> Unit
        }

        return when (val r = apiClient.ingestBatch(token, request)) {
            is SyncCallResult.Ok -> handleAckResponse(entry, r.value, rawAckAffectsSegments = true)
            is SyncCallResult.Retryable -> {
                queue.scheduleRetry(entry, clock())
                diagnostics.onFailure(r.code, r.message, queue.pendingCount())
                diagnostics.logEvent("retry batch=${entry.batchId.take(8)} ${r.code}")
                BatchOutcome.RetryLater(r.code, r.message)
            }
            is SyncCallResult.Permanent -> {
                journal.releaseClaims(entry.segments)
                queue.markFailedPermanent(entry)
                diagnostics.onFailure(r.code, r.message, queue.pendingCount())
                diagnostics.logEvent("permanent batch=${entry.batchId.take(8)} ${r.code}")
                BatchOutcome.Halted(r.code, r.message)
            }
            is SyncCallResult.CredentialsInvalid -> {
                // Batch stays PENDING (scheduled retry) for after re-pairing; nothing is lost.
                queue.scheduleRetry(entry, clock())
                diagnostics.markAwaitingPairing()
                diagnostics.logEvent("credentials-invalid ${r.code}")
                BatchOutcome.Halted(r.code, r.message)
            }
        }
    }

    /** A daily/sleep/rr batch. Same taxonomy as HR, EXCEPT permanence skips the window instead of halting sync. */
    private suspend fun sendFamilyBatch(token: String, entry: QueueEntry): BatchOutcome {
        val request: FamilyRequest = when (entry.family) {
            QueueFamily.DAILY -> {
                val rows = repository.dailyRowsInWindow(entry.dayFromInclusive!!, entry.dayToInclusive!!)
                val items = rows.flatMap { repository.toItems(it) }
                // Rows vanished, or every metric column of every row is null (an empty NOOP day):
                // nothing to send — consume the window so it never re-mints.
                if (items.isEmpty()) return consumeEmptyWindow(entry)
                FamilyRequest.Daily(
                    DailyObservationsRequestDto(
                        batchId = entry.batchId,
                        schemaVersion = SyncContract.FAMILY_SCHEMA_VERSION,
                        decoderVersion = apiClient.config.decoderVersion,
                        items = items,
                    ),
                )
            }
            QueueFamily.SLEEP -> {
                val rows = repository.sleepRowsInWindow(entry.obsFromTsInclusive, entry.obsToTsInclusive)
                if (rows.isEmpty()) return consumeEmptyWindow(entry)
                FamilyRequest.Sleep(
                    SleepSessionsRequestDto(
                        batchId = entry.batchId,
                        schemaVersion = SyncContract.FAMILY_SCHEMA_VERSION,
                        decoderVersion = apiClient.config.decoderVersion,
                        sessions = rows.map { repository.toDto(it) },
                    ),
                )
            }
            QueueFamily.RR -> {
                val rows = repository.rrRowsInWindow(entry.obsFromTsInclusive, entry.obsToTsInclusive)
                if (rows.isEmpty()) return consumeEmptyWindow(entry)
                FamilyRequest.Rr(
                    RrIntervalsRequestDto(
                        batchId = entry.batchId,
                        schemaVersion = SyncContract.FAMILY_SCHEMA_VERSION,
                        decoderVersion = apiClient.config.decoderVersion,
                        records = rows.map { repository.toDto(it) },
                    ),
                )
            }
            QueueFamily.HR -> throw IllegalStateException("HR family uses sendHrBatch")
        }

        val validation = when (request) {
            is FamilyRequest.Daily -> Validators.validateDailyObservations(request.dto)
            is FamilyRequest.Sleep -> Validators.validateSleepSessions(request.dto)
            is FamilyRequest.Rr -> Validators.validateRrIntervals(request.dto)
        }
        return when (validation) {
            is Validation.Invalid -> {
                // Local bug or corrupt local rows: skip THIS window, loudly, and keep every other
                // family flowing. Unlike an HR-batch halt, one family's bad data must not stop
                // the others' acks (watermark skip is what prevents a re-mint loop on this window).
                parkFamilyEntry(entry, "LOCAL_VALIDATION", validation.reason)
                BatchOutcome.Parked
            }
            Validation.Valid -> when (val r = postFamily(token, request)) {
                is SyncCallResult.Ok -> handleAckResponse(entry, r.value, rawAckAffectsSegments = false)
                is SyncCallResult.Retryable -> {
                    queue.scheduleRetry(entry, clock())
                    diagnostics.onFailure(r.code, r.message, queue.pendingCount())
                    diagnostics.logEvent("retry family=${entry.family} batch=${entry.batchId.take(8)} ${r.code}")
                    BatchOutcome.RetryLater(r.code, r.message)
                }
                is SyncCallResult.Permanent -> {
                    parkFamilyEntry(entry, r.code, r.message)
                    BatchOutcome.Parked
                }
                is SyncCallResult.CredentialsInvalid -> {
                    // Batch stays PENDING (scheduled retry) for after re-pairing; nothing is lost.
                    queue.scheduleRetry(entry, clock())
                    diagnostics.markAwaitingPairing()
                    diagnostics.logEvent("credentials-invalid family=${entry.family} ${r.code}")
                    BatchOutcome.Halted(r.code, r.message)
                }
            }
        }
    }

    private fun postFamily(token: String, request: FamilyRequest): SyncCallResult<IngestAckDto> =
        when (request) {
            is FamilyRequest.Daily -> apiClient.postDailyObservations(token, request.dto)
            is FamilyRequest.Sleep -> apiClient.postSleepSessions(token, request.dto)
            is FamilyRequest.Rr -> apiClient.postRrIntervals(token, request.dto)
        }

    /** Rows vanished locally before send (pruned/merged away): consume the window, no request. */
    private fun consumeEmptyWindow(entry: QueueEntry): BatchOutcome {
        advanceFamilyWatermark(entry)
        queue.markAcked(entry, clock())
        diagnostics.logEvent("skip-empty family=${entry.family} batch=${entry.batchId.take(8)}")
        return BatchOutcome.Acked(0)
    }

    /** Shared Ok-path handling for both HR and family batches. */
    private fun handleAckResponse(entry: QueueEntry, ack: IngestAckDto, rawAckAffectsSegments: Boolean): BatchOutcome {
        if (!ack.accepted) {
            if (rawAckAffectsSegments) journal.releaseClaims(entry.segments)
            if (entry.family == QueueFamily.HR) {
                queue.markFailedPermanent(entry)
                val why = ack.warnings.joinToString("; ")
                diagnostics.logEvent("permanent batch=${entry.batchId.take(8)} rejected: $why")
                diagnostics.onFailure("REJECTED", why, queue.pendingCount())
                return BatchOutcome.Halted("REJECTED", "server rejected batch")
            }
            parkFamilyEntry(entry, "REJECTED", ack.warnings.joinToString("; "))
            return BatchOutcome.Parked
        }
        val recordsAcked = ack.recordsInserted + ack.recordsDuplicate
        applyAck(entry, recordsAcked, ack.rawAck, ack.rawBytesStored, ack.serverTime)
        return BatchOutcome.Acked(recordsAcked)
    }

    /**
     * Park a family batch as failed-permanent AND skip its window (advance the family watermark
     * past it). The skip is what keeps the drain alive: without it the next pass would re-mint the
     * same window as a fresh entry and mint-fail forever. Skipping loses exactly this window's
     * rows, is logged loudly, and never blocks the other families. Deliberate deviation from the
     * HR family's halt-on-permanent — see SYNC-NOTES §3.5.
     */
    private fun parkFamilyEntry(entry: QueueEntry, code: String, message: String) {
        queue.markFailedPermanent(entry)
        advanceFamilyWatermark(entry)
        diagnostics.onFailure(code, message, queue.pendingCount())
        diagnostics.logEvent(
            "permanent family=${entry.family} batch=${entry.batchId.take(8)} $code — window SKIPPED: ${message.take(120)}",
        )
    }

    /** Advance the family's own watermark to the entry's window end (monotonic; ack or skip). */
    private fun advanceFamilyWatermark(entry: QueueEntry) {
        when (entry.family) {
            QueueFamily.HR -> watermark.advanceHrTs(entry.obsToTsInclusive)
            QueueFamily.DAILY -> entry.dayToInclusive?.let { watermark.advanceDailyDay(it) }
            QueueFamily.SLEEP -> watermark.advanceSleepTs(entry.obsToTsInclusive)
            QueueFamily.RR -> watermark.advanceRrTs(entry.obsToTsInclusive)
        }
    }

    /** Post-ack state mutation: watermark, server-authorized raw prune, queue, counters. */
    private fun applyAck(
        entry: QueueEntry,
        recordsAcked: Long,
        rawAck: Boolean,
        rawBytesStored: Long,
        serverTime: String,
    ) {
        advanceFamilyWatermark(entry)
        if (entry.segments.isNotEmpty()) {
            if (rawAck) {
                journal.pruneSegments(entry.segments) // server-authorized prune — the ONLY call site
            } else {
                journal.releaseClaims(entry.segments) // frames re-attach to a later batch
            }
        }
        queue.markAcked(entry, clock())
        val bytesAcked = if (rawAck) rawBytesStored else 0L
        diagnostics.onAck(queue.pendingCount(), recordsAcked, bytesAcked, serverTime)
        diagnostics.logEvent(
            "ack family=${entry.family} batch=${entry.batchId.take(8)} records=$recordsAcked rawAck=$rawAck " +
                "segmentsPruned=${if (rawAck) entry.segments.size else 0}",
        )
    }

    private sealed class BatchOutcome {
        data class Acked(val records: Long) : BatchOutcome()

        /** Family batch permanently parked and its window skipped; drain continues with the rest. */
        object Parked : BatchOutcome()

        data class RetryLater(val code: String, val message: String) : BatchOutcome()
        data class Halted(val code: String, val message: String) : BatchOutcome()
    }

    /** Type-safe wrapper so family validation/post dispatch stays exhaustive over the families. */
    private sealed class FamilyRequest {
        data class Daily(val dto: DailyObservationsRequestDto) : FamilyRequest()
        data class Sleep(val dto: SleepSessionsRequestDto) : FamilyRequest()
        data class Rr(val dto: RrIntervalsRequestDto) : FamilyRequest()
    }

    private companion object {
        /** Bounds one pass so a huge backlog interleaves with cancellation checks, not one mega-run. */
        const val MAX_BATCHES_PER_PASS = 8
    }
}
