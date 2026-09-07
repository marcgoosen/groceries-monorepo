package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class TopicTest {
    @ParameterizedTest(name = "{0}")
    @EnumSource(Topic::class)
    fun `It should parse back the configuration key of every topic`(topic: Topic) {
        // Given
        // When
        val parsed = Topic.parse(topic.value)

        assertThat(parsed).isEqualTo(topic)
    }

    @Test
    fun `It should not parse a key that belongs to no topic`() {
        // Given
        // When
        val parsed = Topic.parse("deliveries")

        assertThat(parsed).isNull()
    }

    @Test
    fun `It should not parse a missing key`() {
        // Given
        // When
        val parsed = Topic.parse(null)

        assertThat(parsed).isNull()
    }
}
