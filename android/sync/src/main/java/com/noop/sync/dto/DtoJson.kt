package com.noop.sync.dto

import kotlinx.serialization.json.Json

/*
 * The ONE Json instance for the sync module (fork addition).
 *
 * ignoreUnknownKeys = false is deliberate and load-bearing: the wire contract is frozen, so an
 * unknown key in a server response means contract drift and must fail the parse loudly rather than
 * be silently dropped. Pretty printing is off — bodies are machine-to-machine.
 */
object DtoJson {
    val json: Json = Json {
        ignoreUnknownKeys = false
        isLenient = false
        encodeDefaults = true
        prettyPrint = false
    }
}
