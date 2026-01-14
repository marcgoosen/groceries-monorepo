plugins {
    id("com.diffplug.spotless")
}

spotless {
    kotlin {
        targetExclude("**/build/generated/sources/**/")
        ktlint()
    }
    kotlinGradle {
        ktlint()
    }

    yaml {
        target("**/*.yml", "**/*.yaml", "**/*.yml.example")
        targetExclude("helm/**")
        leadingTabsToSpaces(2)
        jackson()
    }
}
