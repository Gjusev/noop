package com.noop.sync.dto

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/*
 * Pairing + error envelopes of the frozen Somatriq contract (fork addition).
 */

/** POST /api/v1/pairing/confirm body. The 8-char code comes from the Somatriq web app. */
@Serializable
data class PairingConfirmRequestDto(
    @SerialName("pairing_code") val pairingCode: String,
    @SerialName("device_name") val deviceName: String,
)

/** Successful pairing — the device token this app presents as a Bearer credential thereafter. */
@Serializable
data class PairingConfirmResponseDto(
    @SerialName("device_id") val deviceId: String,
    @SerialName("token") val token: String,
    @SerialName("token_type") val tokenType: String,
    @SerialName("scopes") val scopes: List<String>,
    @SerialName("issued_at") val issuedAt: String,
    /** null = token does not expire (revocation is the server's lever instead). */
    @SerialName("expires_at") val expiresAt: String? = null,
)

/**
 * Error envelope for every non-2xx response. `error_code` is one of the frozen codes in
 * [com.noop.sync.SyncContract]; classification into retryable/permanent/credentials happens in
 * [com.noop.sync.api.SyncApiClient].
 */
@Serializable
data class ErrorDto(
    @SerialName("error_code") val errorCode: String,
    @SerialName("message") val message: String,
    @SerialName("details") val details: List<String> = emptyList(),
)

/**
 * Client-side view of the pairing/auth state (NOT a frozen server endpoint — the shape of any
 * future server status endpoint may differ; this is what the app UI shows).
 */
@Serializable
data class AuthStatusDto(
    @SerialName("device_id") val deviceId: String? = null,
    @SerialName("paired") val paired: Boolean,
    @SerialName("token_type") val tokenType: String? = null,
    @SerialName("scopes") val scopes: List<String> = emptyList(),
    @SerialName("issued_at") val issuedAt: String? = null,
    @SerialName("expires_at") val expiresAt: String? = null,
)

/**
 * Descriptive device metadata for diagnostics/logs only (NOT part of the frozen ingest or pairing
 * bodies — never sent unless a future frozen endpoint asks for it).
 */
@Serializable
data class DeviceInfoDto(
    @SerialName("decoder_version") val decoderVersion: String,
    @SerialName("app_version") val appVersion: String,
    @SerialName("platform") val platform: String,
    @SerialName("model") val model: String,
)
