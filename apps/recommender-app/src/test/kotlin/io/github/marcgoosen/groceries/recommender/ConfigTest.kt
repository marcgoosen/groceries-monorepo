package io.github.marcgoosen.groceries.recommender

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.each
import assertk.assertions.isEqualTo
import assertk.assertions.key
import assertk.assertions.none
import org.junit.jupiter.api.Test

class ConfigTest {
    private val config = loadConfig()

    @Test
    fun `It should load the packaged configuration`() {
        // Given
        // When
        val main = config.main

        assertThat(main).isEqualTo(
            Config.MainConfig(
                applicationId = "groceries-recommender",
                port = 8080,
                logbackConfigFile = "logback.xml",
                createTopics = true,
                startSimulator = true,
            ),
        )
    }

    @Test
    fun `It should resolve every placeholder it declares`() {
        // Given
        // When
        val values = config.kafka.values + config.topics.name.values + config.main.logbackConfigFile

        assertThat(values).each { it.transform { value -> value.contains("\${") }.isEqualTo(false) }
    }

    @Test
    fun `It should build the Kafka application id from the application id and version`() {
        // Given
        // When
        val kafka = config.kafka

        assertThat(kafka).key("application.id").isEqualTo("groceries-recommender_v1")
    }

    @Test
    fun `It should name a topic for every topic the application uses`() {
        // Given
        // When
        val names = config.topics.name

        assertThat(names.keys).isEqualTo(setOf("products", "orders", "related-products"))
        assertThat(names).key("orders").isEqualTo("groceries.orders.v1")
    }

    @Test
    fun `It should only ask to create topics it has a name for`() {
        // Given
        // When
        val unnamed = config.topics.create.keys - config.topics.name.keys

        assertThat(unnamed).isEqualTo(emptySet())
    }

    @Test
    fun `It should not log a credential it was given`() {
        // Given
        val withCredentials = config.copy(
            kafka = config.kafka + ("schema.registry.basic.auth.user.info" to "key:secret"),
        )

        // When
        val scrambled = withCredentials.scramble()

        assertThat(scrambled.kafka.values).none { it.contains("key:secret") }
    }
}
