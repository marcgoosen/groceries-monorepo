import org.gradle.accessors.dm.LibrariesForLibs

// https://github.com/gradle/gradle/issues/15383
val libs = the<LibrariesForLibs>()

plugins {
    // Apply the common convention plugin for shared build configuration between library and application projects.
    id("buildlogic.kotlin-common-conventions")

    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

dependencies {
    implementation(libs.logback.classic)
}

