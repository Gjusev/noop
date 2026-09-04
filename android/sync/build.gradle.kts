// Somatriq sync module (fork addition; not upstream NOOP code).
//
// An additive, local-first sync layer: captures raw BLE frames pre-decoder into a zstd-compressed
// on-device journal, ships decoded HR observations + raw journal batches to a self-hosted Somatriq
// server, and NEVER lets remote availability endanger wearable data (everything is persisted locally
// first; network is strictly a secondary exporter). No networking code leaks into NOOP's BLE or
// protocol classes — the app hooks this module through a single no-op-by-default capture point.
plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlin.plugin.serialization")
}

android {
    namespace = "com.noop.sync"
    compileSdk = 35

    defaultConfig {
        minSdk = 26
        consumerProguardFiles("consumer-rules.pro")
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }

    // Pure-JVM unit tests: the module is deliberately written so its logic (journal codec, queue,
    // DTO validation, watermark) needs no Android framework — only the credentials store and
    // WorkManager entry points touch android.*.
    testOptions {
        unitTests.isReturnDefaultValues = true
    }
}

dependencies {
    // HTTP: OkHttp is what the host app already ships (AI Coach); no second stack.
    api("com.squareup.okhttp3:okhttp:4.12.0")

    // Coroutines (host app already ships these; pinned to the same versions).
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")

    // Wire contract serialization. Frozen snake_case field names via @SerialName; strict JSON
    // (ignoreUnknownKeys = false) so a contract drift fails loudly on the client, not silently.
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")

    // Raw Journal v1 codec. The AAR carries the Android ABIs (arm64-v8a, armeabi-v7a, x86, x86_64);
    // the plain JAR carries desktop natives and is used ONLY on the unit-test classpath so JVM tests
    // can round-trip compress/decompress without a device. Declaring the JAR as implementation too
    // would dex duplicate classes into the host APK — do not "fix" this into one artifact.
    implementation("com.github.luben:zstd-jni:1.5.7-1@aar")
    testImplementation("com.github.luben:zstd-jni:1.5.7-1")

    // Background sync scheduling (host app pins 2.9.0 — the compileSdk 34 ceiling for this stack).
    implementation("androidx.work:work-runtime-ktx:2.9.0")

    // Device-token storage: Android Keystore-backed encrypted preferences, same artifact the host
    // app already uses for the AI Coach key.
    implementation("androidx.security:security-crypto:1.1.0-alpha06")

    // --- Unit tests (pure JVM) ---
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
}
