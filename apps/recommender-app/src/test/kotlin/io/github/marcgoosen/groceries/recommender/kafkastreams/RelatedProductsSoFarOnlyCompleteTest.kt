package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.RelatedProduct
import io.github.marcgoosen.groceries.recommender.domain.RelatedProductsSoFar
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-products-context"
private const val OUTPUT_TOPIC = "output-products-complete"
class RelatedProductsSoFarOnlyCompleteTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, RelatedProductsSoFar>
    private lateinit var outputTopic: TestOutputTopic<OrderId, RelatedProductsSoFar>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, RelatedProductsSoFar>(INPUT_TOPIC)
                .onlyComplete()
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
            avroSerdes.create<RelatedProductsSoFar>().deserializer(),
        )
    }

    @Test
    fun `It should drop aggregates that are still waiting for a product lookup`() {
        // Given
        val milk = RelatedProduct(Product("p1", "Milk", 2.0), 0.8)
        val complete = RelatedProductsSoFar(relatedProducts = listOf(milk), expectedSize = 1)
        val incomplete = RelatedProductsSoFar(relatedProducts = listOf(milk), expectedSize = 2)

        // When
        inputTopic.pipeInput("o1", incomplete)
        inputTopic.pipeInput("o1", complete)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(complete))
    }
}
