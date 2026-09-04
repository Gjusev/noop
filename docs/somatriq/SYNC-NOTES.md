# Somatriq sync — engineering notes (NOOP fork)

Fork-only documentation. Upstream (ryanbr/noop) does not carry this file or the `android/sync`
module. Everything here concerns branch `somatriq/sync` and descendants.

---

## 1. Recon findings (as of carried-upstream base `7d7702c`)

### 1.1 Android project map

| What | Where / value |
|---|---|
| Project root | `android/` (single module `:app` until this fork added `:sync`) |
| Gradle | 8.7 wrapper, AGP 8.5.2, JDK 17, `dependency-verification` strict (`android/gradle/verification-metadata.xml`) |
| Kotlin | 1.9.24, KSP 1.9.24-1.0.20, Compose compiler 1.5.14 |
| SDK | compileSdk 35, minSdk 26, targetSdk 34 |
| Flavors | `full` (com.noop.whoop) and `demo` (…​.demo), dimension `tier` |
| HTTP stack | OkHttp 4.12.0 (AI Coach) — reused by `:sync`; no Retrofit/Ktor |
| Serialization | none before this fork → kotlinx-serialization-json 1.6.3 added in `:sync` |
| DI | none (manual wiring in `NoopApplication`) — `:sync` follows suit |
| Coroutines | 1.8.1; WorkManager work-runtime-ktx 2.9.0 already a dependency |
| Secret storage | `androidx.security:security-crypto:1.1.0-alpha06` (`SecurePrefs` helper) — reused by `:sync` |
| Room | `WhoopDatabase` v36, 28 entity classes declared (26+ tables incl. `hrSample`, `ppgHrSample`, …) |
| App CI | `.github/workflows/android.yml` (assembleFullDebug + testFullDebugUnitTest, JDK 17) |

### 1.2 The `synced` flag verdict: NOT a usable sync watermark

**Conclusion: do not use `hrSample.synced` as the sync watermark. Use a max-`ts` watermark kept
in the sync module's own state file. The sync layer never writes NOOP's tables.**

Evidence (file:line at base `7d7702c`):

1. `android/app/src/main/java/com/noop/data/Entities.kt:58` — the column's own doc:
   `// v5: per-row upload flag; unused locally, kept for schema parity. Defaults to 0.`
   It exists to keep Room byte-compatible with the Swift/GRDB twin, not as live state.
2. Nothing ever writes it to `1`. Repo-wide search for `synced = 1`, `synced=1`, `SET synced`
   over `com/noop/**` returns zero hits. Inserts go through `WhoopDao.insertHr`
   (`WhoopDao.kt:58-59`, `@Insert(onConflict = OnConflictStrategy.IGNORE)`) with the entity
   default `synced = 0`, and IGNORE means a re-insert of an acked row can never "reset" a flag —
   but nothing ever sets it in the first place.
3. No DAO method exists to mark rows synced. There is no `UPDATE hrSample SET synced` anywhere.
4. The only reader of the column, the PPG-union query `WhoopDao.hrSamples`
   (`WhoopDao.kt:310-321`), SELECTs it but deliberately hardcodes `0 AS synced` for the
   `ppgHrSample` half (line 316) — the flag is not even consistent across the two HR tables.
5. The Room schema is under a parity oracle (`SchemaOracleTest` vs `schema_oracle.json`, which
   the Swift twin checks against GRDB). Writing to NOOP tables from sync would also drag
   cross-platform schema semantics into a fork-only feature.

Watermark implementation: `com.noop.sync.SyncWatermark` — JSON file at
`filesDir/somatriq/watermark.json`, monotonic `advanceHrTs()`, advanced ONLY on a server ack, to
the batch's upper ts bound (which itself is bounded to the rows actually shipped — see
`SyncRepository.nextWindowEnd`), so acked windows stay exactly aligned with shipped rows.

### 1.3 The BLE→protocol seam (where raw capture hooks in)

**Seam: `WhoopBleClient.onInbound(uuid: UUID, bytes: ByteArray)` —
`android/app/src/main/java/com/noop/ble/WhoopBleClient.kt:6739`, specifically the reassembler
loop at (base) line 6809:**

```kotlin
for (frame in reassembler.feed(bytes)) {   // frame: ByteArray, COMPLETE, exactly once per frame
    try {
        RawCapturePoint.capture(frame)      // ← the Somatriq tap (fork addition)
        …
        val parsed = Framing.parseFrame(frame, connectedFamily)   // line ~6824: the decoder call
```

Why here and not inside `Framing.parseFrame` (protocol/Framing.kt:270, the other candidate):

- `onInbound` is the single transport funnel: every complete frame from the WHOOP custom notify
  characteristics (4.0 and 5/MG alike) emerges from `Reassembler.feed` here exactly ONCE — live
  frames, backfill/offload frames, everything.
- `Framing.parseFrame` is called from multiple secondary paths too (the parse-then-route shims at
  WhoopBleClient.kt:7131/9002 kept for legacy callers, and the experimental backfill capture
  writer at ~10521 which re-parses frames). Hooking the decoder would double-journal frames on
  those paths.
- Patching the transport file keeps the protocol package 100% untouched — the spec's "networking
  must not leak into BLE protocol classes" is enforced by construction: the only sync symbol in
  `WhoopBleClient.kt` is `RawCapturePoint`, a pure interface object with a no-op default, and the
  journal write is local disk (no networking reaches the BLE stack, period).

The tap itself is `com.noop.sync.capture.RawCapturePoint` — a process-wide `@Volatile`
`RawCapture` (fun interface) defaulting to `NoopRawCapture`; `capture()` swallows all throwables
so the GATT binder thread can never be crashed from the journal.

---

## 2. Design decisions & deviations

### 2.1 Queue = JSON files, not a second Room DB

`com.noop.sync.queue.FileSyncQueue` keeps one JSON file per batch entry under
`filesDir/somatriq/queue/` (atomic tmp+rename writes). Rationale: NOOP's Room DB is
schema-oracle-locked to the Swift twin; a second small DB in `:sync` adds KSP/Room surface nobody
guards for a handful of tiny rows. Batch payloads never live in the queue — observations are
re-read from NOOP's Room by ts range at send time and raw frames live in journal segment files —
so entries stay a few hundred bytes.

### 2.2 Cadence: 15-minute periodic floor (deviation from the spec's 5 minutes)

WorkManager's periodic floor is 15 min. `SyncManager.schedule()` enqueues the periodic drain at
15 min; the spec's ~5-minute target is met opportunistically via **expedited** one-time syncs
triggered by `SyncManager.onForegroundEvent()` (app foreground, completed sleep/workout events).
The host app can call this hook from its lifecycle points; this milestone wires only startup, so
the foreground hook is available but not yet called from every lifecycle site — see SOMATRIQ-TODOs
in FORK.md. Battery guardrails on every request: `NetworkType.CONNECTED` +
`requiresBatteryNotLow(true)`.

### 2.3 Raw journal durability window (deliberate tradeoff)

Frames are captured into an in-memory buffer and rotated to a zstd segment at ~1 MiB
uncompressed or 5 min (whichever first). Compression and file writes run on a dedicated single
thread (`somatriq-raw-journal`), never on the GATT binder thread. Consequence: a hard process
death can lose at most the un-rotated buffer (≤1 MiB / ≤5 min of frames). Per-frame fsync would
tax the BLE thread for data that still exists on the strap until NOOP acks the backfill.
`SomatriqSyncBridge.onAppBackground()` flushes the buffer proactively.

### 2.4 Prune discipline

Journal segments are deleted ONLY in `RawJournalWriter.pruneSegments`, called ONLY from
`SyncEngine.applyAck` when the ack said `raw_ack = true` for a batch carrying those segments. An
observations-only ack (`raw_ack = false`) releases the segments back to the ready pool so they
re-attach to a later batch — the server's ack is the prune authorization, never a clock.

### 2.5 Idempotency

Batch UUIDs are minted exactly once, at `FileSyncQueue.enqueue`, persisted in the entry file, and
reused for every retry (also after process death — `requeueStaleInFlight()` returns killed
IN_FLIGHT entries to PENDING with the same id). Observation identity is
`source_record_id = "hrSample:<deviceId>:<ts>"`; the server dedupes on it, so a re-sent window
can only ever produce `records_duplicate`, never a double-insert.

### 2.6 What ships in this milestone

`hrSample` only, via `WhoopDao.rawHrSamples` (measured strap HR; deliberately NOT the
PPG-derived union — Somatriq wants sensor observations, and derived estimates would
double-represent the same second). `ts` (unix seconds) maps to ISO-8601 UTC instants
(`Instant.ofEpochSecond(...)` → `"2026-09-04T06:41:00Z"`).

### 2.7 zstd on Android + JVM

`com.github.luben:zstd-jni:1.5.7-1` resolves as TWO artifacts: the **AAR** (classes + arm64-v8a,
armeabi-v7a, x86, x86_64 natives) is the runtime dependency; the **JAR** (desktop natives) is
test-classpath-only. Declaring the JAR as `implementation` too would dex duplicate classes into
the APK — do not merge them.

### 2.8 Dependency-verification metadata

The android build runs with strict Gradle dependency verification
(`android/gradle/verification-metadata.xml`). The new artifacts (kotlinx-serialization-core/json
1.6.3, the Kotlin serialization plugin + marker, zstd-jni 1.5.7-1 jar/aar) were added with
checksums via `./gradlew --write-verification-metadata sha256` — regenerate the same way after
any dependency bump, and review the diff.

### 2.9 Non-frozen DTOs

`AuthStatusDto` and `DeviceInfoDto` are client-side shapes for status/diagnostics only. Only the
ingest batch, ingest ack, pairing confirm (req/resp), and error envelopes are frozen by the
contract; the test fixtures under `android/sync/src/test/resources/somatriq/` pin them verbatim.

### 2.10 Known capture caveats

- The WHOOP 5/MG experimental backfill capture (`writeWhoop5BackfillCapture`) re-parses frames;
  since the Somatriq tap sits upstream at `onInbound`, it is unaffected — each frame journals
  exactly once.
- `WhoopObservationSource.maxHrTs()` reports NOOP's `latestHrSampleTs`, which coalesces
  `hrSample` with PPG-derived `ppgHrSample` (WhoopDao.kt:951-972). Informational only — the
  engine batches from real rows (`nextWindowEnd`), never from this value.
