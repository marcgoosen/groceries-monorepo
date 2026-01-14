plugins {
    `kotlin-dsl`
}

dependencies {
    // https://github.com/gradle/gradle/issues/15383
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:${libs.versions.kotlin.core.get()}")
    implementation("com.diffplug.spotless:spotless-plugin-gradle:${libs.versions.spotless.get()}")
    implementation("org.jetbrains.kotlinx:kover-gradle-plugin:${libs.versions.kover.get()}")
}
