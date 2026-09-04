package com.noop.sync

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/*
 * Sync state + diagnostics (fork addition).
 *
 * Observability contract: what the UI sees is counters and timestamps — never frame contents,
 * never tokens, never health data. The recent-event ring buffer is bounded and redacted the same
 * way (error codes and counts only).
 */

enum class SyncStatus {
    /** Nothing to do / nothing running. */
    IDLE,

    /** A sync pass is executing. */
    SYNCING,

    /** No valid device token; network sync paused (capture continues locally). */
    AWAITING_PAIRING,

    /** Last pass ended in failure; retries are scheduled. */
    ERROR,
}

data class SyncState(
    val status: SyncStatus = SyncStatus.IDLE,
    val lastErrorCode: String? = null,
    val lastErrorMessage: String? = null,
    val batchesAcked: Long = 0,
    val recordsAcked: Long = 0,
    val rawBytesAcked: Long = 0,
    val consecutiveFailures: Int = 0,
    val lastAckAtMs: Long? = null,
    val lastAckServerTime: String? = null,
    val pendingBatches: Int = 0,
    val readySegments: Int = 0,
)

/** Mutable diagnostics holder; expose to UI via [state] only. */
class SyncDiagnostics {

    private val _state = MutableStateFlow(SyncState())
    val state: StateFlow<SyncState> = _state.asStateFlow()

    private val events = ArrayDeque<String>(EVENT_LIMIT)

    fun snapshot(): SyncState = _state.value

    fun updateStatus(status: SyncStatus) = mutate { it.copy(status = status) }

    fun markSyncing(pendingBatches: Int) = mutate {
        it.copy(status = SyncStatus.SYNCING, pendingBatches = pendingBatches)
    }

    fun markAwaitingPairing() = mutate {
        it.copy(status = SyncStatus.AWAITING_PAIRING, lastErrorCode = null, lastErrorMessage = null)
    }

    fun onAck(pendingBatches: Int, records: Long, rawBytes: Long, serverTime: String) = mutate {
        it.copy(
            status = SyncStatus.IDLE,
            batchesAcked = it.batchesAcked + 1,
            recordsAcked = it.recordsAcked + records,
            rawBytesAcked = it.rawBytesAcked + rawBytes,
            consecutiveFailures = 0,
            lastAckAtMs = System.currentTimeMillis(),
            lastAckServerTime = serverTime,
            lastErrorCode = null,
            lastErrorMessage = null,
            pendingBatches = pendingBatches,
        )
    }

    fun onFailure(code: String, message: String, pendingBatches: Int) = mutate {
        it.copy(
            status = SyncStatus.ERROR,
            lastErrorCode = code,
            lastErrorMessage = message.take(200),
            consecutiveFailures = it.consecutiveFailures + 1,
            pendingBatches = pendingBatches,
        )
    }

    fun onSegmentsReady(readySegments: Int) = mutate { it.copy(readySegments = readySegments) }

    fun logEvent(line: String) {
        synchronized(events) {
            events.addFirst("${System.currentTimeMillis()} $line")
            while (events.size > EVENT_LIMIT) events.removeLast()
        }
    }

    /** Bounded, redacted recent events (newest first). */
    fun recentEvents(): List<String> = synchronized(events) { events.toList() }

    private fun mutate(f: (SyncState) -> SyncState) {
        _state.value = f(_state.value)
    }

    private companion object {
        const val EVENT_LIMIT = 64
    }
}
