package io.github.marcgoosen.groceries.recommender

import assertk.assertThat
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.Test

class ConfigExtensionsTest {
    private val config = Config(
        main = Config.MainConfig(logbackConfigFile = "logback.xml"),
        kafka = mapOf(
            "bootstrap.servers" to "broker:9092",
            "sasl.jaas.config" to "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"me\";",
            "schema.registry.basic.auth.user.info" to "key:secret",
            "ssl.truststore.password" to "hunter2",
            "schema.registry.url" to "http://schema-registry:8081",
        ),
        topics = Config.Topics(name = mapOf("orders" to "groceries.orders.v1")),
    )

    @Test
    fun `It should mask every credential while leaving the rest readable`() {
        // Given
        // When
        val scrambled = config.scramble()

        assertThat(scrambled).isEqualTo(
            config.copy(
                kafka = mapOf(
                    "bootstrap.servers" to "broker:9092",
                    "sasl.jaas.config" to "********",
                    "schema.registry.basic.auth.user.info" to "********",
                    "ssl.truststore.password" to "********",
                    "schema.registry.url" to "http://schema-registry:8081",
                ),
            ),
        )
    }

    @Test
    fun `It should leave an unset credential empty rather than implying one exists`() {
        // Given
        val withoutAuth = config.copy(kafka = mapOf("schema.registry.basic.auth.user.info" to ""))

        // When
        val scrambled = withoutAuth.scramble()

        assertThat(scrambled).isEqualTo(withoutAuth)
    }
}
