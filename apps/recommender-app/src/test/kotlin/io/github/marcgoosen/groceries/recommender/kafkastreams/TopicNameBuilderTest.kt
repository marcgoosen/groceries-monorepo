package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isEqualTo
import assertk.assertions.isFailure
import assertk.assertions.isInstanceOf
import org.junit.jupiter.api.Test

class TopicNameBuilderTest {
    private val topicNameBuilder = TopicNameBuilder(
        mapOf(
            "orders" to "groceries.orders.v1",
            "products" to "groceries.products.v1",
        ),
    )

    @Test
    fun `It should resolve the configured name for a topic`() {
        // Given
        // When
        val name = topicNameBuilder.build(Topic.ORDER)

        assertThat(name).isEqualTo("groceries.orders.v1")
    }

    @Test
    fun `It should fail for a topic that has no configured name`() {
        // Given
        // When
        assertThat(runCatching { topicNameBuilder.build(Topic.RELATED_PRODUCTS) })
            .isFailure()
            .isInstanceOf(IllegalArgumentException::class)
            .hasMessage("Topic related-products not found")
    }
}
