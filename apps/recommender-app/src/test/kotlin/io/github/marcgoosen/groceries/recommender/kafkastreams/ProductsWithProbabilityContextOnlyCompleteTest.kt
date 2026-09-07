package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbabilityContext
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-products-context"
private const val OUTPUT_TOPIC = "output-products-complete"

@OptIn(ExperimentalAvro4kApi::class)
class ProductsWithProbabilityContextOnlyCompleteTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, ProductsWithProbabilityContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, ProductsWithProbabilityContext>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, ProductsWithProbabilityContext>(INPUT_TOPIC)
                .onlyComplete()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<ProductsWithProbabilityContext>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<ProductsWithProbabilityContext>().deserializer(),
        )
    }

    @Test
    fun `It should drop aggregates that are still waiting for a product lookup`() {
        // Given
        val milk = ProductWithProbability(Product("p1", "Milk", 2.0), 0.8)
        val complete = ProductsWithProbabilityContext(productsWithProbabilities = listOf(milk), expectedSize = 1)
        val incomplete = ProductsWithProbabilityContext(productsWithProbabilities = listOf(milk), expectedSize = 2)

        // When
        inputTopic.pipeInput("o1", incomplete)
        inputTopic.pipeInput("o1", complete)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(complete))
    }
}
