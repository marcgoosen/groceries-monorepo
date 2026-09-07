package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.RelatedProduct
import io.github.marcgoosen.groceries.recommender.domain.RelatedProducts
import io.github.marcgoosen.groceries.recommender.domain.RelatedProductsSoFar
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-context"
private const val OUTPUT_TOPIC = "output-products"
class RemoveEmptyTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, RelatedProductsSoFar>
    private lateinit var outputTopic: TestOutputTopic<OrderId, RelatedProducts>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, RelatedProductsSoFar>(INPUT_TOPIC)
                .removeEmpty()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<RelatedProductsSoFar>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<RelatedProducts>().deserializer(),
        )
    }

    @Test
    fun `It should leave out the products that could not be looked up`() {
        // Given
        val milk = RelatedProduct(Product("p1", "Milk", 2.0), 0.8)
        val context = RelatedProductsSoFar(
            relatedProducts = listOf(milk, null),
            expectedSize = 2,
        )

        // When
        inputTopic.pipeInput("o1", context)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(RelatedProducts(listOf(milk))))
    }

    @Test
    fun `It should emit an empty recommendation when no product could be looked up`() {
        // Given
        val context = RelatedProductsSoFar(
            relatedProducts = listOf(null, null),
            expectedSize = 2,
        )

        // When
        inputTopic.pipeInput("o2", context)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(RelatedProducts(emptyList())))
    }
}
