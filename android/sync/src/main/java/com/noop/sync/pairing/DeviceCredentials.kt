package com.noop.sync.pairing

import android.content.Context
import android.content.SharedPreferences
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey
import com.noop.sync.dto.PairingConfirmResponseDto

/*
 * Device-token storage (fork addition).
 *
 * EncryptedSharedPreferences (Android Keystore-backed master key, AES-256-GCM) — the same
 * security-crypto artifact the host app already uses for the AI Coach key, so the dependency and
 * its hardening history are shared. The token is written ONLY here and read ONLY as a Bearer
 * header value; it is never logged and never included in diagnostics state.
 */

data class DeviceCredentials(
    val deviceId: String,
    val token: String,
    val tokenType: String,
    val scopes: List<String>,
    val issuedAt: String?,
    val expiresAt: String?,
)

interface DeviceTokenStore {
    fun save(credentials: DeviceCredentials)
    fun load(): DeviceCredentials?
    fun token(): String?
    fun clear()
}

/** Keystore-backed store; safe to hold for the process lifetime (create is cached). */
class EncryptedDeviceTokenStore(context: Context) : DeviceTokenStore {

    private val prefs: SharedPreferences by lazy {
        val masterKey = MasterKey.Builder(context)
            .setKeyScheme(MasterKey.KeyScheme.AES256_GCM)
            .build()
        EncryptedSharedPreferences.create(
            context,
            PREFS_FILE,
            masterKey,
            EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
            EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM,
        )
    }

    override fun save(credentials: DeviceCredentials) {
        prefs.edit()
            .putString(KEY_DEVICE_ID, credentials.deviceId)
            .putString(KEY_TOKEN, credentials.token)
            .putString(KEY_TOKEN_TYPE, credentials.tokenType)
            .putStringSet(KEY_SCOPES, credentials.scopes.toSet())
            .putString(KEY_ISSUED_AT, credentials.issuedAt)
            .putString(KEY_EXPIRES_AT, credentials.expiresAt)
            .apply()
    }

    override fun load(): DeviceCredentials? {
        val token = prefs.getString(KEY_TOKEN, null) ?: return null
        return DeviceCredentials(
            deviceId = prefs.getString(KEY_DEVICE_ID, "").orEmpty(),
            token = token,
            tokenType = prefs.getString(KEY_TOKEN_TYPE, "device").orEmpty(),
            scopes = prefs.getStringSet(KEY_SCOPES, null)?.toList() ?: emptyList(),
            issuedAt = prefs.getString(KEY_ISSUED_AT, null),
            expiresAt = prefs.getString(KEY_EXPIRES_AT, null),
        )
    }

    override fun token(): String? = prefs.getString(KEY_TOKEN, null)

    override fun clear() {
        prefs.edit().clear().apply()
    }

    private companion object {
        const val PREFS_FILE = "somatriq_device_credentials"
        const val KEY_DEVICE_ID = "device_id"
        const val KEY_TOKEN = "token"
        const val KEY_TOKEN_TYPE = "token_type"
        const val KEY_SCOPES = "scopes"
        const val KEY_ISSUED_AT = "issued_at"
        const val KEY_EXPIRES_AT = "expires_at"
    }
}

fun PairingConfirmResponseDto.toCredentials(): DeviceCredentials = DeviceCredentials(
    deviceId = deviceId,
    token = token,
    tokenType = tokenType,
    scopes = scopes,
    issuedAt = issuedAt,
    expiresAt = expiresAt,
)
