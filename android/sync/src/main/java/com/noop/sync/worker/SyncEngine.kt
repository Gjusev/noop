package com.noop.sync.worker

import com.noop.sync.SyncContract
import com.noop.sync.SyncDiagnostics
import com.noop.sync.SyncRepository
import com.noop.sync.SyncWatermark
import com.noop.sync.api.SyncApiClient
import com.noop.sync.api.SyncCallResult
import com.noop.sync.capture.RawJournalWriter
import com.noop.sync.dto.IngestBatchRequestDto
import com.noop.sync.dto.Validation
import com.noop.sync.dto.Validators
import com.noop.sync.pairing.DeviceTokenStore
import com.noop.sync.queue.FileSyncQueue
import com.noop.sync.queue.QueueEntry

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
 *  - No skipped rows: each batch's ts window is bounded to what fits in MAX_RECORDS_PER_BATCH, so
 *    acking the window advances the watermark over exactly the rows that shipped.
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

    /** Terminal condition (credentials/permanent/local bug); sync pauses until user action. */
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
     * Mint the next batch from whatever is new: observations beyond the watermark (window bounded
     * to [SyncContract.MAX_RECORDS_PER_BATCH] rows) and/or ready journal segments. Returns null
     * when there is nothing to ship at all.
     */
    private suspend fun enqueueNextWindow(): QueueEntry? {
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
        diagnostics.logEvent("enqueue batch=${entry.batchId.take(8)} obs≤${entry.obsToTsInclusive} segments=${segments.size}")
        return entry
    }

    private suspend fun sendBatch(token: String, entry: QueueEntry): BatchOutcome {
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
            is SyncCallResult.Ok -> {
                val ack = r.value
                if (!ack.accepted) {
                    journal.releaseClaims(entry.segments)
                    queue.markFailedPermanent(entry)
                    val why = ack.warnings.joinToString("; ")
                    diagnostics.logEvent("permanent batch=${entry.batchId.take(8)} rejected: $why")
                    diagnostics.onFailure("REJECTED", why, queue.pendingCount())
                    BatchOutcome.Halted("REJECTED", "server rejected batch")
                } else {
                    val recordsAcked = ack.recordsInserted + ack.recordsDuplicate
                    applyAck(entry, recordsAcked, ack.rawAck, ack.rawBytesStored, ack.serverTime)
                    BatchOutcome.Acked(recordsAcked)
                }
            }
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

    /** Post-ack state mutation: watermark, server-authorized raw prune, queue, counters. */
    private fun applyAck(
        entry: QueueEntry,
        recordsAcked: Long,
        rawAck: Boolean,
        rawBytesStored: Long,
        serverTime: String,
    ) {
        watermark.advanceHrTs(entry.obsToTsInclusive)
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
            "ack batch=${entry.batchId.take(8)} records=$recordsAcked rawAck=$rawAck " +
                "segmentsPruned=${if (rawAck) entry.segments.size else 0}",
        )
    }

    private sealed class BatchOutcome {
        data class Acked(val records: Long) : BatchOutcome()
        data class RetryLater(val code: String, val message: String) : BatchOutcome()
        data class Halted(val code: String, val message: String) : BatchOutcome()
    }

    private companion object {
        /** Bounds one pass so a huge backlog interleaves with cancellation checks, not one mega-run. */
        const val MAX_BATCHES_PER_PASS = 8
    }
}
