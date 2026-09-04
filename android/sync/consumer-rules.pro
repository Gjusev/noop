# Somatriq sync module consumer rules (fork addition).
#
# The host app ships UNMINIFIED (see app/build.gradle.kts), so these rules are belt-and-braces for
# any future minified consumer. kotlinx.serialization needs its serializers kept; zstd-jni and
# OkHttp ship their own rules. The @Serializable DTOs are reflected on by the serialization
# compiler plugin (compile-time codegen, not runtime reflection), so only the plugin-generated
# companions strictly need keeping.

-if @kotlinx.serialization.Serializable class **
-keepclassmembers class <1> {
    static <1>$Companion Companion;
}

-if @kotlinx.serialization.Serializable class ** {
    static **$* *;
}
-keepclassmembers class <2>$<3> {
    kotlinx.serialization.KSerializer serializer(...);
}

-keep,includedescriptorclasses class com.noop.sync.**$$serializer { *; }
-keepclassmembers class com.noop.sync.** {
    *** Companion;
}
