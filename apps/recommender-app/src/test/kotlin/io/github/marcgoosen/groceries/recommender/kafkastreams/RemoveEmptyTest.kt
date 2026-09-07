package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbabilityContext
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-context"
private const val OUTPUT_TOPIC = "output-products"

@OptIn(ExperimentalAvro4kApi::class)
class RemoveEmptyTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, ProductsWithProbabilityContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, ProductsWithProbability>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, ProductsWithProbabilityContext>(INPUT_TOPIC)
                .removeEmpty()
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
            avroSerdes.create<ProductsWithProbability>().deserializer(),
        )
    }

    @Test
    fun `It should leave out the products that could not be looked up`() {
        // Given
        val milk = ProductWithProbability(Product("p1", "Milk", 2.0), 0.8)
        val context = ProductsWithProbabilityContext(
            productsWithProbabilities = listOf(milk, null),
            expectedSize = 2,
        )

        // When
        inputTopic.pipeInput("o1", context)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(ProductsWithProbability(listOf(milk))))
    }

    @Test
    fun `It should emit an empty recommendation when no product could be looked up`() {
        // Given
        val context = ProductsWithProbabilityContext(
            productsWithProbabilities = listOf(null, null),
            expectedSize = 2,
        )

        // When
        inputTopic.pipeInput("o2", context)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(ProductsWithProbability(emptyList())))
    }
}
