plugins {
    // These are not in buildSrc classpath yet, so we can define versions here via alias
    alias(libs.plugins.kotlinx.serialization) apply false
    alias(libs.plugins.ktor) apply false
}

allprojects {
    group = "io.github.marcgoosen.groceries"
    version = "1.0.0-SNAPSHOT"

    repositories {
        mavenCentral()
        maven("https://packages.confluent.io/maven")
    }
}
