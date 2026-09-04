package com.noop.sync.api

import com.noop.sync.SyncContract
import com.noop.sync.dto.DtoJson
import com.noop.sync.dto.ErrorDto
import com.noop.sync.dto.IngestAckDto
import com.noop.sync.dto.IngestBatchRequestDto
import com.noop.sync.dto.PairingConfirmRequestDto
import com.noop.sync.dto.PairingConfirmResponseDto
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import java.io.IOException
import java.util.concurrent.TimeUnit

/*
 * HTTP client for the Somatriq server (fork addition).
 *
 * Two endpoints, one error taxonomy. Every non-2xx is mapped to exactly one of:
 *   [SyncCallResult.Retryable]     — try the SAME batch again later (same batch_id; the server
 *                                    dedupes on it, so a repeat is always safe)
 *   [SyncCallResult.Permanent]     — the server rejected this batch for good; park it as
 *                                    failed-permanent, surface the code, never auto-resend
 *   [SyncCallResult.CredentialsInvalid] — the device token is bad/expired/revoked; sync pauses
 *                                    until re-pairing
 *
 * Tokens ride only in the Authorization header and are never logged.
 */

data class SyncConfig(
    val serverBaseUrl: String = SyncContract.DEFAULT_SERVER_URL,
    /** e.g. "noop-android/11.1.1+somatriq" — built by the app from its version. */
    val decoderVersion: String,
    val connectTimeoutMs: Long = 10_000,
    val readTimeoutMs: Long = 30_000,
)

sealed class SyncCallResult<out T> {
    data class Ok<T>(val value: T) : SyncCallResult<T>()
    data class Retryable(val code: String, val message: String) : SyncCallResult<Nothing>()
    data class Permanent(val code: String, val message: String) : SyncCallResult<Nothing>()
    data class CredentialsInvalid(val code: String, val message: String) : SyncCallResult<Nothing>()

    companion object {
        fun networkError(message: String): SyncCallResult<Nothing> =
            Retryable("NETWORK", message)
    }
}

class SyncApiClient(
    val config: SyncConfig,
    private val client: OkHttpClient = defaultClient(config),
) {

    private val jsonMediaType = "application/json; charset=utf-8".toMediaType()

    /** POST {server}/api/v1/ingest/batches with the device token. */
    fun ingestBatch(deviceToken: String, request: IngestBatchRequestDto): SyncCallResult<IngestAckDto> =
        execute(
            path = SyncContract.PATH_INGEST_BATCHES,
            body = DtoJson.json.encodeToString(
                IngestBatchRequestDto.serializer(), request,
            ),
            token = deviceToken,
        ) { DtoJson.json.decodeFromString(IngestAckDto.serializer(), it) }

    /** POST {server}/api/v1/pairing/confirm (no token — the code IS the credential). */
    fun confirmPairing(pairingCode: String, deviceName: String): SyncCallResult<PairingConfirmResponseDto> =
        execute(
            path = SyncContract.PATH_PAIRING_CONFIRM,
            body = DtoJson.json.encodeToString(
                PairingConfirmRequestDto.serializer(),
                PairingConfirmRequestDto(pairingCode, deviceName),
            ),
            token = null,
        ) { DtoJson.json.decodeFromString(PairingConfirmResponseDto.serializer(), it) }

    // --- plumbing ---

    private inline fun <reified T> execute(
        path: String,
        body: String,
        token: String?,
        parse: (String) -> T,
    ): SyncCallResult<T> {
        val url = config.serverBaseUrl.trimEnd('/') + path
        val builder = Request.Builder()
            .url(url)
            .post(body.toRequestBody(jsonMediaType))
            .header("Accept", "application/json")
        token?.let { builder.header("Authorization", "Bearer $it") }

        val response: Response = try {
            client.newCall(builder.build()).execute()
        } catch (e: IOException) {
            return SyncCallResult.networkError(e.message ?: "network failure")
        }

        response.use { resp ->
            val responseBody = resp.body?.string().orEmpty()
            if (resp.isSuccessful) {
                return try {
                    SyncCallResult.Ok(parse(responseBody))
                } catch (e: Exception) {
                    // 2xx but unparseable = contract drift (ignoreUnknownKeys=false bites here) —
                    // NOT safely retryable with the same expectations; park it visibly.
                    SyncCallResult.Permanent("UNPARSEABLE_SUCCESS", describe(e))
                }
            }
            val error = parseError(responseBody)
            val code = error?.errorCode ?: "HTTP_${resp.code}"
            val message = error?.message ?: ("HTTP " + resp.code + bodyPreview(responseBody))
            return when {
                resp.code == 401 || resp.code == 403 -> SyncCallResult.CredentialsInvalid(code, message)
                isCredentialsCode(code) -> SyncCallResult.CredentialsInvalid(code, message)
                isPermanentCode(code) -> SyncCallResult.Permanent(code, message)
                resp.code >= 500 -> SyncCallResult.Retryable(code, message)
                else -> SyncCallResult.Retryable(code, message)
            }
        }
    }

    private fun parseError(body: String): ErrorDto? = try {
        DtoJson.json.decodeFromString(ErrorDto.serializer(), body)
    } catch (_: Exception) {
        null
    }

    private fun describe(e: Exception): String = "${e.javaClass.simpleName}: ${e.message?.take(200)}"

    private fun bodyPreview(body: String): String = if (body.isBlank()) "" else " — " + body.take(200)

    private fun isCredentialsCode(code: String): Boolean = code in setOf(
        SyncContract.ERR_AUTHENTICATION,
        SyncContract.ERR_INVALID_CREDENTIALS,
        SyncContract.ERR_DEVICE_REVOKED,
        SyncContract.ERR_DEVICE_NOT_FOUND,
    )

    private fun isPermanentCode(code: String): Boolean = code in setOf(
        SyncContract.ERR_VALIDATION,
        SyncContract.ERR_PERMANENT,
        SyncContract.ERR_IDEMPOTENCY_CONFLICT,
        SyncContract.ERR_AUTHORIZATION,
        SyncContract.ERR_NOT_FOUND,
        SyncContract.ERR_ACCOUNT_EXISTS,
        SyncContract.ERR_PAIRING_CODE_INVALID,
        SyncContract.ERR_PAIRING_CODE_EXPIRED,
        SyncContract.ERR_PAIRING_SESSION_NOT_FOUND,
    )

    companion object {
        fun defaultClient(config: SyncConfig): OkHttpClient = OkHttpClient.Builder()
            .connectTimeout(config.connectTimeoutMs, TimeUnit.MILLISECONDS)
            .readTimeout(config.readTimeoutMs, TimeUnit.MILLISECONDS)
            .writeTimeout(config.readTimeoutMs, TimeUnit.MILLISECONDS)
            // One retry on a fresh connection for transient TLS/TCP resets; batch-level retries are
            // the queue's job, not the transport's.
            .retryOnConnectionFailure(true)
            .build()
    }
}
