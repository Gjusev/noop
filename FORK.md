# NOOP → Somatriq fork manifest

This repository is a fork of **NOOP** carrying the **Somatriq** mobile sync layer. This file is
the fork's bookkeeping: what was added, what was touched, how to re-merge upstream, and what the
license obligates.

## Remotes of record

| Role | Repository |
|---|---|
| Upstream of record | https://github.com/ryanbr/noop (NOOP proper) |
| Origin (this fork's push target) | https://github.com/Gjusev/noop |
| Somatriq server (separate repo) | Somatriq monorepo (`compose.yml` topology) |

- Carried-upstream base: **`7d7702c8a2e666b5aba827940df3caaed5386d97`**
  (`build: staging 434 / iOS 315 for the next testing build`) — the upstream `main` HEAD at the
  time branch `somatriq/sync` was cut. `git remote add upstream https://github.com/ryanbr/noop.git`
  once, then this must always equal or descend from `upstream/main`.
- Work branch for the sync slice: **`somatriq/sync`**.

## Files ADDED by the fork

### Gradle module `android/sync` (package `com.noop.sync`)

| File | Purpose |
|---|---|
| `android/sync/build.gradle.kts` | Android library module: OkHttp (app's existing stack), kotlinx-serialization-json, zstd-jni (AAR runtime / JAR test), work-runtime-ktx, security-crypto, coroutines, JUnit |
| `android/sync/consumer-rules.pro` | ProGuard keeps for the serialization-generated serializers (host ships unminified; belt-and-braces) |
| `src/main/java/com/noop/sync/SyncContract.kt` | Frozen wire constants: schema "2", journal v1, codec zstd, endpoints, bpm/size gates, rotation + retry policy, error-code vocabulary |
| `src/main/java/com/noop/sync/SyncState.kt` | `SyncStatus`/`SyncState` + `SyncDiagnostics` (StateFlow, counters, bounded redacted event log) |
| `src/main/java/com/noop/sync/SyncRepository.kt` | `ObservationSource` interface (app implements over its Room DAO), `HrObservation`, DTO mapping, ts-window batching, monotonic `SyncWatermark` |
| `src/main/java/com/noop/sync/SyncRuntime.kt` | Process-wide installed object graph + on-disk layout under `filesDir/somatriq/` |
| `src/main/java/com/noop/sync/SyncManager.kt` | Facade: pair/unpair, expedited `forceSyncNow()`, 15-min periodic `schedule()`, `onForegroundEvent()`, battery/network constraints |
| `src/main/java/com/noop/sync/dto/DtoJson.kt` | The one strict `Json` instance (`ignoreUnknownKeys = false`) |
| `src/main/java/com/noop/sync/dto/IngestDtos.kt` | Frozen ingest DTOs (`IngestRecordDto`, `RawPayloadDto`, `IngestBatchRequestDto`, `IngestAckDto`) |
| `src/main/java/com/noop/sync/dto/PairingDtos.kt` | Pairing req/resp, error envelope, client-side `AuthStatusDto`/`DeviceInfoDto` |
| `src/main/java/com/noop/sync/dto/Validators.kt` | Client mirrors of the server gates: bpm 20.0–250.0, ts must carry UTC offset, 8 MiB payload cap, schema gates |
| `src/main/java/com/noop/sync/api/SyncApiClient.kt` | OkHttp client: ingest + pairing endpoints; error_code → Retryable/Permanent/CredentialsInvalid taxonomy |
| `src/main/java/com/noop/sync/capture/RawCapturePoint.kt` | `fun interface RawCapture`, no-op default, process-wide never-throwing tap — the only sync symbol the BLE stack sees |
| `src/main/java/com/noop/sync/capture/JournalCodec.kt` | Raw Journal v1 pure-JVM codec: `u32_be len \| u64_be epoch_ms \| frame`, zstd round-trip |
| `src/main/java/com/noop/sync/capture/SegmentStore.kt` | Segment storage abstraction + `FileSegmentStore` (atomic tmp+rename writes, restart-safe sequence) |
| `src/main/java/com/noop/sync/capture/RawJournalWriter.kt` | The journal: in-memory append, rotation at ~1 MiB/5 min, IO off the BLE thread, claim/build/prune/release with server-authorized pruning only |
| `src/main/java/com/noop/sync/pairing/DeviceCredentials.kt` | Keystore-backed `EncryptedDeviceTokenStore` (token never logged) + in-memory store type |
| `src/main/java/com/noop/sync/pairing/PairingManager.kt` | Pairing-code confirm + token persistence, typed outcomes |
| `src/main/java/com/noop/sync/queue/SyncQueue.kt` | Durable JSON-file batch queue: batch UUID minted once at enqueue, backoff table, crash-safe state machine |
| `src/main/java/com/noop/sync/worker/SyncEngine.kt` | The drain state machine (plain Kotlin, JVM-testable): window batching, send, ack effects, prune authorization, cancel-safety |
| `src/main/java/com/noop/sync/worker/SyncWorker.kt` | CoroutineWorker adapter (cancellation → StopSignal) |
| `src/test/java/com/noop/sync/capture/RawJournalWriterTest.kt` | Journal round-trips byte-exact, big-endian header layout, sha256-of-compressed, rotation by size and time, claim/prune/release, restart |
| `src/test/java/com/noop/sync/dto/DtoSerializationTest.kt` | Frozen fixtures parsed field-for-field, round-trips, snake_case emission, strict unknown/missing-key rejection |
| `src/test/java/com/noop/sync/dto/ValidatorTest.kt` | bpm bounds, naive-ts rejection, 8 MiB cap, schema gates |
| `src/test/java/com/noop/sync/queue/FileSyncQueueTest.kt` | Batch-id stability, due/FIFO, crash requeue, restart persistence, backoff cap, watermark monotonicity |
| `src/test/resources/somatriq/*.json` | The frozen wire examples, verbatim, as test resources |

### App + repo (outside the module)

| File | Purpose |
|---|---|
| `android/app/src/main/java/com/noop/somatriq/SomatriqSyncBridge.kt` | Fork wiring: builds the object graph at startup, installs the raw tap, schedules sync only when paired; read-only `WhoopObservationSource` over `WhoopDao.rawHrSamples` |
| `.github/workflows/android-ci.yml` | Fork CI: `:sync:test` + `:app:assembleFullRelease -PstagingRelease`, APK artifact on main |
| `docs/somatriq/SYNC-NOTES.md` | Recon evidence (seam, `synced` verdict), design decisions, deviations |
| `FORK.md` | This file |

## Files TOUCHED by the fork (patches — keep these diffs tiny)

| File | Change |
|---|---|
| `android/settings.gradle.kts` | `include(":sync")` |
| `android/build.gradle.kts` | declare `org.jetbrains.kotlin.plugin.serialization` 1.9.24 `apply false` |
| `android/app/build.gradle.kts` | `implementation(project(":sync"))` |
| `android/app/src/main/java/com/noop/ble/WhoopBleClient.kt` | +1 import, +1 call: `RawCapturePoint.capture(frame)` as the first statement inside the existing try of the reassembler loop in `onInbound` (~line 6809 at base) — the pre-decoder seam |
| `android/app/src/main/java/com/noop/NoopApplication.kt` | +1 call in `onCreate`: `com.noop.somatriq.SomatriqSyncBridge.install(this)` (self-guarding) |
| `android/gradle/verification-metadata.xml` | checksums for the new dependencies (kotlinx-serialization 1.6.3, kotlin serialization plugin 1.9.24, zstd-jni 1.5.7-1 jar+aar, work-runtime transitives) — regenerate with `./gradlew <tasks> --write-verification-metadata sha256` |
| `android/app/gradle.lockfile` | app's `dependencyLocking` state extended with the sync module's transitive artifacts (zstd-jni, kotlinx-serialization) — regenerate with `./gradlew :app:dependencies --write-locks`, per the procedure comment in `app/build.gradle.kts` |

## Re-merge procedure (weekly, per Somatriq ADR 0004)

Mechanical merge from upstream; the fork's surface is designed to make it boring:

```bash
git fetch upstream
git checkout -b merge/upstream-<date> upstream/main   # or merge into somatriq/sync directly
git merge upstream/main
# Expect conflicts ONLY in the touched files above:
#   - settings.gradle.kts, build.gradle.kts, app/build.gradle.kts: re-apply the one-liners
#   - WhoopBleClient.kt: re-apply the import + the single RawCapturePoint.capture(frame) line
#     (if onInbound moved, re-find the reassembler loop: `for (frame in reassembler.feed(bytes))`)
#   - NoopApplication.kt: re-apply the SomatriqSyncBridge.install(this) line in onCreate
#   - verification-metadata.xml: regenerate instead of hand-merging:
#       ./gradlew :sync:test :app:assembleFullRelease -PstagingRelease \
#           --write-verification-metadata sha256
#   - gradle.lockfile: regenerate with `./gradlew :app:dependencies --write-locks`
# Everything else (the whole android/sync module, FORK.md, docs/somatriq/) is fork-only and
# cannot conflict.
./gradlew :sync:test && ./gradlew :app:assembleFullDebug   # before pushing
```

If upstream ever restructures `onInbound` or the reassembler loop, the invariant to preserve is:
**every COMPLETE frame must pass through `RawCapturePoint.capture(frame)` exactly once, before
decode, on the inbound path.** `Framing.parseFrame` is NOT the hook (secondary parse paths would
double-journal) — see SYNC-NOTES §1.3.

## PolyForm-NC obligations

- NOOP's own code and documentation are licensed under the **PolyForm Noncommercial 1.0.0**
  license (`LICENSE`); copyright 2026 NoopApp. The fork preserves that license, NOOP's copyright
  and attribution notices (`LICENSE`, `NOTICE`, `ATTRIBUTION.md`, `DISCLAIMER.md`), and does not
  remove or alter any upstream headers in touched files.
- **All fork-added code (the `android/sync` module, `SomatriqSyncBridge.kt`, CI workflow, docs)
  is likewise noncommercial-only** — it is a derivative work within the same PolyForm-NC licensed
  tree. Do not relicense it permissively.
- Per NOOP's license scope note: the BLE protocol facts documented in this repository remain
  uncopyrightable factual information; nothing in the sync module changes that.

## SOMATRIQ-TODOs

- `// SOMATRIQ-TODO(wiring)` in `SomatriqSyncBridge`: no UI ships in this milestone —
  `SyncManager.pair()` / `status` are ready for a settings screen, and `onForegroundEvent()` +
  `onAppBackground()` are not yet called from the app's lifecycle (only startup wiring exists).
  Deliberately left unwired: every additional call site is another merge-conflict surface.
- More observation streams (R-R, SpO2, skin temp, sleep sessions) slot in as additional
  `ObservationSource` methods; only HrSample ships now.
- A device-token expiry check (`expires_at`) is stored but not enforced client-side (the frozen
  contract shows `expires_at: null`; revocation is server-side and surfaces as
  CredentialsInvalid).

## Pointers

- Engineering notes + recon evidence: `docs/somatriq/SYNC-NOTES.md`
- Wire contract fixtures: `android/sync/src/test/resources/somatriq/`
- Somatriq spec §34–39 (sync rules) and ADR 0004 (weekly mechanical upstream merges) live in the
  Somatriq monorepo, not in this fork.
