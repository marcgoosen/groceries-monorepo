plugins {
    id("buildlogic.kotlin-application-conventions")
    alias(libs.plugins.kotlinx.serialization)
}

// Kept out of `check` so `./gradlew build` stays fast and needs no Docker; CI runs it as its own step.
val integrationTest = sourceSets.create("integrationTest") {
    compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    runtimeClasspath += output + compileClasspath
}

configurations["integrationTestImplementation"].extendsFrom(configurations.testImplementation.get())
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())

dependencies {
    implementation(projects.libs.shared)
    implementation(libs.kafka.streams)
    implementation(libs.slf4j)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.faker)

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

    "integrationTestImplementation"(libs.bundles.test.containers)
}

application {
    mainClass.set("io.github.marcgoosen.groceries.recommender.AppKt")
}

tasks.register<Test>("integrationTest") {
    description = "Runs the pipeline against a real Kafka broker and Schema Registry."
    group = "verification"
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)
}

tasks.named<JavaExec>("run") {
    environment("LOGBACK_CONFIG_FILE", "logback-local.xml")
}
