package com.noop.sync.pairing

import com.noop.sync.api.SyncApiClient
import com.noop.sync.api.SyncCallResult
import com.noop.sync.dto.PairingConfirmResponseDto

/*
 * Pairing flow (fork addition): exchange the 8-char code shown by the Somatriq web app for a
 * device token, then store it. Pairing is the ONLY step that must happen before any network sync;
 * capture keeps working unpaired.
 */

sealed class PairingOutcome {
    data class Paired(val response: PairingConfirmResponseDto) : PairingOutcome()
    data class InvalidCode(val code: String, val message: String) : PairingOutcome()
    data class ExpiredCode(val code: String, val message: String) : PairingOutcome()
    data class Retryable(val code: String, val message: String) : PairingOutcome()
    data class Failed(val code: String, val message: String) : PairingOutcome()
}

class PairingManager(
    private val apiClient: SyncApiClient,
    private val tokenStore: DeviceTokenStore,
) {

    /**
     * Confirm a pairing code. On success the token is persisted and the outcome is [PairingOutcome.Paired];
     * every failure mode is typed so the UI can say precisely what went wrong.
     */
    fun confirm(pairingCode: String, deviceName: String): PairingOutcome {
        val code = pairingCode.trim().uppercase()
        if (!PAIRING_CODE_REGEX.matches(code)) {
            return PairingOutcome.InvalidCode("FORMAT", "pairing code must be 8 characters")
        }
        return when (val r = apiClient.confirmPairing(code, deviceName)) {
            is SyncCallResult.Ok -> {
                tokenStore.save(r.value.toCredentials())
                PairingOutcome.Paired(r.value)
            }
            is SyncCallResult.CredentialsInvalid ->
                PairingOutcome.Failed(r.code, r.message)
            is SyncCallResult.Permanent -> when (r.code) {
                "PAIRING_CODE_INVALID" -> PairingOutcome.InvalidCode(r.code, r.message)
                "PAIRING_CODE_EXPIRED" -> PairingOutcome.ExpiredCode(r.code, r.message)
                else -> PairingOutcome.Failed(r.code, r.message)
            }
            is SyncCallResult.Retryable -> PairingOutcome.Retryable(r.code, r.message)
        }
    }

    fun isPaired(): Boolean = tokenStore.token() != null

    fun unpair() = tokenStore.clear()

    private companion object {
        /** 8 characters, letters+digits — the frozen Somatriq pairing-code shape ("8CHARSUP"). */
        val PAIRING_CODE_REGEX = Regex("^[A-Z0-9]{8}$")
    }
}
