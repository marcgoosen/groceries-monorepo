import io.ktor.plugin.features.DockerImageRegistry

plugins {
    id("buildlogic.kotlin-application-conventions")
    alias(libs.plugins.kotlinx.serialization)
    alias(libs.plugins.ktor)
}

// Both are kept out of `check` so `./gradlew build` stays fast and needs no Docker; CI runs them as their
// own steps. integrationTest starts its own containers; e2eTest expects a compose stack to be running.
val integrationTest = sourceSets.create("integrationTest") {
    compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    runtimeClasspath += output + compileClasspath
}

val e2eTest = sourceSets.create("e2eTest") {
    compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    runtimeClasspath += output + compileClasspath
}

configurations["integrationTestImplementation"].extendsFrom(configurations.testImplementation.get())
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())
configurations["e2eTestImplementation"].extendsFrom(configurations.testImplementation.get())
configurations["e2eTestRuntimeOnly"].extendsFrom(configurations.testRuntimeOnly.get())

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

ktor {
    docker {
        // jreVersion defaults to 21, which already matches jvmToolchain(21); the base image is
        // eclipse-temurin:21-jre.
        localImageName.set("recommender-app")
        imageTag.set(providers.gradleProperty("imageTag").orElse("latest"))
        externalRegistry.set(
            DockerImageRegistry.externalRegistry(
                username = providers.environmentVariable("GHCR_USERNAME"),
                password = providers.environmentVariable("GHCR_TOKEN"),
                project = provider { "recommender-app" },
                hostname = provider { "ghcr.io" },
                namespace = provider { "marcgoosen" },
            ),
        )
    }
}

// Kover otherwise wires every Test task into koverGenerateArtifact, and so into `check`, which drags both
// container-dependent suites into `./gradlew build`. It also measures their source sets as if they were
// production code, which both inflates coverage when they run and deflates it when they do not.
kover {
    currentProject {
        instrumentation {
            disabledForTestTasks.addAll("integrationTest", "e2eTest")
        }
        sources {
            excludedSourceSets.addAll("integrationTest", "e2eTest")
        }
    }
}

// Ktor's DSL covers neither of these, and its setupJib tasks leave them alone.
jib {
    container {
        ports = listOf("8080")
        user = "1000"
    }
}

tasks.register<Test>("integrationTest") {
    description = "Runs the pipeline against a real Kafka broker and Schema Registry."
    group = "verification"
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    useJUnitPlatform()
    shouldRunAfter(tasks.test)
}

tasks.register<Test>("e2eTest") {
    description = "Asserts against the packaged image running in a compose stack; start the stack first."
    group = "verification"
    testClassesDirs = e2eTest.output.classesDirs
    classpath = e2eTest.runtimeClasspath
    useJUnitPlatform()
    // The stack it asserts against is outside Gradle's view, so an unchanged project says nothing about
    // whether this would still pass.
    outputs.upToDateWhen { false }
}

tasks.named<JavaExec>("run") {
    environment("LOGBACK_CONFIG_FILE", "logback-local.xml")
}
