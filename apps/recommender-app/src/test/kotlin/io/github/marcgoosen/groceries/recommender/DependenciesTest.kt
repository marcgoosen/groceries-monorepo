package io.github.marcgoosen.groceries.recommender

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import assertk.assertions.isNotEmpty
import assertk.assertions.isSuccess
import io.github.marcgoosen.groceries.recommender.kafkastreams.Topic
import org.apache.kafka.streams.KafkaStreams
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

/**
 * Dependencies is the composition root: it holds no logic of its own, so what is worth asserting is that the parts
 * are wired to the same configuration. It builds real Kafka clients, which construct without reaching a broker.
 */
class DependenciesTest {
    companion object {
        @JvmStatic
        @AfterAll
        fun releaseClients() {
            Dependencies.streams.close()
            Dependencies.admin.close()
            Dependencies.simulator.close()
        }
    }

    @Test
    fun `It should expose the packaged configuration`() {
        // Given
        // When
        val config = Dependencies.config

        assertThat(config).isEqualTo(loadConfig())
    }

    @Test
    fun `It should resolve topic names from the configured names`() {
        // Given
        val configured = Dependencies.config.topics.name

        // When
        val resolved = Topic.entries.associate { it.value to Dependencies.topicNameBuilder.build(it) }

        assertThat(resolved).isEqualTo(configured)
    }

    @Test
    fun `It should build a topology that reads and writes the configured topics`() {
        // Given
        val names = Dependencies.config.topics.name

        // When
        val described = Dependencies.topology.describe().toString()

        assertThat(described).contains(names.getValue("orders"))
        assertThat(described).contains(names.getValue("products"))
        assertThat(described).contains(names.getValue("related-products"))
    }

    @Test
    fun `It should build a stream client that has not been started`() {
        // Given
        // When
        val state = Dependencies.streams.state()

        assertThat(state).isEqualTo(KafkaStreams.State.CREATED)
    }

    @Test
    fun `It should tolerate configuration keys it does not know when logging the config`() {
        // Given
        val payload = """{"main":{"logbackConfigFile":"logback.xml"},"kafka":{},"topics":{"name":{}},"extra":1}"""

        // When
        val decoded = Dependencies.json.decodeFromString<Config>(payload)

        assertThat(decoded.main.logbackConfigFile).isEqualTo("logback.xml")
    }

    @Test
    fun `It should expose a registry that can be scraped`() {
        // Given
        Dependencies.prometheusRegistry.counter("test_counter").increment()

        // When
        val scrape = Dependencies.prometheusRegistry.scrape()

        assertThat(scrape).isNotEmpty()
        assertThat(scrape).contains("test_counter")
    }

    @Test
    fun `It should build serdes against the configured schema registry`() {
        // Given
        // When
        val serdeConfig = Dependencies.avroSerdes.serdeConfig

        assertThat(serdeConfig).isEqualTo(Dependencies.config.kafka)
    }

    @Test
    fun `It should build a simulator that closes cleanly`() {
        // Given
        // When
        assertThat(runCatching { Dependencies.simulator }).isSuccess()
    }
}
