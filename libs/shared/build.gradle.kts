plugins {
    id("buildlogic.kotlin-library-conventions")
    alias(libs.plugins.kotlinx.serialization)
}

dependencies {
    api(libs.kafka.streams)
    implementation(libs.kotlin.logging)
    api(libs.kotlinx.datetime)
    api(libs.kotlinx.serialization.json)
    api(libs.hoplite.core)
    api(libs.hoplite.yaml)

    api(libs.kotlin.reflect)

    // Implementation dependencies
    api(libs.bundles.avro4k.serdes)

    // Test dependencies
    testImplementation(libs.kafka.streams.test.utils)
    testImplementation(libs.faker)
}
