package com.noop.sync.capture

/*
 * The single hook the host app needs (fork addition).
 *
 * [RawCapturePoint.capture] is called with each COMPLETE reconstructed BLE frame, pre-decoder, on
 * the GATT binder thread. The default is a no-op, so an unpaired / uninstalled app pays one volatile
 * read and a call that returns. Everything else — journaling, compression, network — lives behind
 * the installed implementation and must never throw back into the BLE loop.
 */

/** A sink for raw (pre-decoder) BLE frames. */
fun interface RawCapture {
    /** Append one complete frame. Implementations MUST NOT throw. */
    fun capture(frame: ByteArray)
}

/** The stock no-op sink: capture is installed but disabled, or the module is not wired. */
object NoopRawCapture : RawCapture {
    override fun capture(frame: ByteArray) = Unit
}

/**
 * Process-wide capture point. The host app calls [capture] from its BLE inbound seam; Somatriq
 * wiring calls [install] once at startup. Keeping the indirection in an object means the app patch
 * is exactly one line and holds no sync state of its own.
 */
object RawCapturePoint : RawCapture {

    @Volatile
    private var impl: RawCapture = NoopRawCapture

    fun install(capture: RawCapture) {
        impl = capture
    }

    fun reset() {
        impl = NoopRawCapture
    }

    override fun capture(frame: ByteArray) {
        // Belt-and-braces: a failure in the journal must NEVER crash the BLE inbound loop, whatever
        // the installed implementation promised.
        try {
            impl.capture(frame)
        } catch (_: Throwable) {
            // Deliberately swallowed (see RawCapture.capture contract).
        }
    }
}
