package com.noop.sync

/*
 * Somatriq wire contract — frozen constants (fork addition, not upstream NOOP code).
 *
 * Every value here mirrors the Somatriq server's frozen contract (spec §M2 wire). Field NAMES live
 * on the DTOs as @SerialName annotations; the JSON is configured once in [com.noop.sync.dto.DtoJson]
 * with ignoreUnknownKeys = false so an unexpected server change fails loudly at the parse site
 * instead of silently dropping fields.
 */

object SyncContract {

    /** Server of record. Overridable at runtime for debugging; NO secrets in code (public repo). */
    const val DEFAULT_SERVER_URL: String = "https://somatriq.mokka-dev.de"

    // --- Endpoints (under {server}/api/v1) ---
    const val PATH_INGEST_BATCHES: String = "/api/v1/ingest/batches"
    const val PATH_PAIRING_CONFIRM: String = "/api/v1/pairing/confirm"
    const val PATH_INGEST_DAILY_OBSERVATIONS: String = "/api/v1/ingest/daily-observations"
    const val PATH_INGEST_SLEEP_SESSIONS: String = "/api/v1/ingest/sleep-sessions"
    const val PATH_INGEST_RR_INTERVALS: String = "/api/v1/ingest/rr-intervals"

    // --- Wire schema versions (frozen) ---
    const val SCHEMA_VERSION: String = "2"

    /**
     * The three single-family endpoints (daily-observations / sleep-sessions / rr-intervals) froze
     * independently of the HR+raw batch endpoint and carry schema_version "1".
     */
    const val FAMILY_SCHEMA_VERSION: String = "1"
    const val JOURNAL_VERSION: Int = 1
    const val CODEC_ZSTD: String = "zstd"

    /** decoder_version prefix; the app appends its own version, e.g. `noop-android/11.1.1+somatriq`. */
    const val DECODER_VERSION_PREFIX: String = "noop-android"

    // --- Client-side validation gates (mirrored from the server) ---
    const val BPM_MIN: Double = 20.0
    const val BPM_MAX: Double = 250.0

    /** R-R interval gates for /ingest/rr-intervals (rr_ms inclusive range). */
    const val RR_MS_MIN: Int = 200
    const val RR_MS_MAX: Int = 2500

    /**
     * Hard cap on R-R records per request. R-R is the high-frequency family (~1 beat/s all night),
     * so it batches at 20k/request — a full night is ~30k beats, i.e. two requests, and a large
     * backlog drains as consecutive queue entries.
     */
    const val RR_MAX_RECORDS_PER_BATCH: Int = 20_000

    /** Daily-metric rows per request (each row expands to ≤ [DAILY_METRICS] items, so ≤ 6.5k items). */
    const val DAILY_MAX_ROWS_PER_BATCH: Int = 366

    /** Sleep sessions per request (sessions are heavy: each carries its full stage timeline). */
    const val SLEEP_MAX_SESSIONS_PER_BATCH: Int = 60

    /** Maximum DECODED base64 payload bytes the server accepts (8 MiB). */
    const val MAX_PAYLOAD_BYTES: Long = 8L * 1024 * 1024

    /** Hard cap on records per batch (client-side throttle; server accepts more but no need to). */
    const val MAX_RECORDS_PER_BATCH: Int = 1_000

    // --- Frozen metric vocabulary for /ingest/daily-observations ---
    // Order matches NOOP's DailyMetric column order (Entities.kt) so the mapping table in
    // SyncRepository is auditable line-by-line against the entity.
    val DAILY_METRICS: List<String> = listOf(
        "total_sleep_min", "efficiency", "deep_min", "rem_min", "light_min", "disturbances",
        "resting_hr", "avg_hrv", "recovery", "strain", "exercise_count", "spo2_pct",
        "skin_temp_dev_c", "resp_rate_bpm", "steps", "active_kcal_est", "avg_sdnn", "skin_temp_c",
    )

    /** Frozen stage vocabulary for /ingest/sleep-sessions `stages[].state`. */
    val SLEEP_STATES: List<String> = listOf("awake", "light", "deep", "rem")

    // --- Raw journal policy ---
    /** Rotate a journal segment at ~1 MiB of UNCOMPRESSED entries. */
    const val SEGMENT_ROTATE_BYTES: Int = 1 shl 20

    /** …or after 5 minutes with the current segment open, whichever comes first. */
    const val SEGMENT_ROTATE_MS: Long = 5L * 60 * 1000

    /** Segment file name shape: seg-<zero-padded seq>.zst under filesDir/somatriq/raw. */
    const val SEGMENT_PREFIX: String = "seg-"
    const val SEGMENT_SUFFIX: String = ".zst"

    // --- Retry policy (own table on top of WorkManager's scheduling) ---
    const val RETRY_BACKOFF_BASE_MS: Long = 30L * 1000
    const val RETRY_BACKOFF_MAX_MS: Long = 2L * 60 * 60 * 1000

    /**
     * WorkManager periodic floor is 15 minutes; the Somatriq spec targets ~5. The periodic schedule
     * uses 15 min and the gap is closed with expedited one-time syncs on app-foreground and after
     * completed sleep/workout events. See docs/somatriq/SYNC-NOTES.md.
     */
    const val PERIODIC_SYNC_MINUTES: Long = 15

    // --- Error codes (frozen server vocabulary) ---
    const val ERR_VALIDATION: String = "VALIDATION"
    const val ERR_AUTHENTICATION: String = "AUTHENTICATION"
    const val ERR_AUTHORIZATION: String = "AUTHORIZATION"
    const val ERR_IDEMPOTENCY_CONFLICT: String = "IDEMPOTENCY_CONFLICT"
    const val ERR_RETRYABLE: String = "RETRYABLE"
    const val ERR_PERMANENT: String = "PERMANENT"
    const val ERR_NOT_FOUND: String = "NOT_FOUND"
    const val ERR_ACCOUNT_EXISTS: String = "ACCOUNT_EXISTS"
    const val ERR_INVALID_CREDENTIALS: String = "INVALID_CREDENTIALS"
    const val ERR_PAIRING_CODE_INVALID: String = "PAIRING_CODE_INVALID"
    const val ERR_PAIRING_CODE_EXPIRED: String = "PAIRING_CODE_EXPIRED"
    const val ERR_PAIRING_SESSION_NOT_FOUND: String = "PAIRING_SESSION_NOT_FOUND"
    const val ERR_DEVICE_NOT_FOUND: String = "DEVICE_NOT_FOUND"
    const val ERR_DEVICE_REVOKED: String = "DEVICE_REVOKED"
}
