plugins {
    id("buildlogic.kotlin-application-conventions")
    alias(libs.plugins.kotlinx.serialization)
}

dependencies {
    implementation(project(":libs:shared"))
    implementation(libs.kafka.streams)
    implementation(libs.slf4j)
    implementation(libs.kotlinx.serialization.json)

    // Logging and monitoring dependencies
    implementation(libs.kotlin.logging)
    implementation(libs.micrometer.core)
    implementation(libs.micrometer.registry.prometheus)
    implementation(libs.logback.classic)
    implementation(libs.logback.logstash.encoder)

    // Ktor dependencies
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.server.metrics.micrometer)

    // Test dependencies
    testImplementation(libs.kafka.streams.test.utils)
    testImplementation(libs.ktor.server.test.host)
    implementation(libs.faker)
}

application {
    mainClass.set("io.github.marcgoosen.groceries.recommender.AppKt")
}
