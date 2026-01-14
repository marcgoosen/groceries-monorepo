import org.gradle.accessors.dm.LibrariesForLibs

// https://github.com/gradle/gradle/issues/15383
val libs = the<LibrariesForLibs>()

plugins {
    id("org.jetbrains.kotlin.jvm")
    id("buildlogic.formatting-conventions")
    id("org.jetbrains.kotlinx.kover")
}

dependencies {
    implementation(platform(libs.kotlin.bom))
    implementation(platform(libs.kotlinx.coroutines.bom))
    implementation(platform(libs.kotest.bom))

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation(libs.mockk)
    testImplementation("io.kotest:kotest-assertions-core")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(libs.versions.java.get().toInt())
    compilerOptions {
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}
