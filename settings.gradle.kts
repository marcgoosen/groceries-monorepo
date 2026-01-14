pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        maven("https://packages.confluent.io/maven")
    }
}

rootProject.name = "groceries-monorepo"

include("libs:shared")
include("apps:recommender-app")
