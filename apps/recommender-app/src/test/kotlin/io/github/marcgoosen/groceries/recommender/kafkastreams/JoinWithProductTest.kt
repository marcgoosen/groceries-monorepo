package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.ProbabilityContext
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbabilityContext
import io.github.marcgoosen.groceries.recommender.domain.withProbability
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val CONTEXT_INPUT_TOPIC = "input-context"
private const val PRODUCT_INPUT_TOPIC = "input-products"
private const val OUTPUT_TOPIC = "output-joined"
class JoinWithProductTest : BaseTopologyTest() {
    private lateinit var contextInputTopic: TestInputTopic<ProductId, ProbabilityContext>
    private lateinit var productInputTopic: TestInputTopic<ProductId, Product>
    private lateinit var outputTopic: TestOutputTopic<ProductId, ProductWithProbabilityContext>

    @BeforeEach
    fun onSetup() {
        setup {
            val productTable = streamsBuilder.table<ProductId, Product>(PRODUCT_INPUT_TOPIC)

            streamsBuilder.stream<ProductId, ProbabilityContext>(CONTEXT_INPUT_TOPIC)
                .joinWithProduct(productTable)
                .to(OUTPUT_TOPIC)
        }

        contextInputTopic = topologyTestDriver.createInputTopic(
            CONTEXT_INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<ProbabilityContext>().serializer(),
        )

        productInputTopic = topologyTestDriver.createInputTopic(
            PRODUCT_INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<Product>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<ProductWithProbabilityContext>().deserializer(),
        )
    }

    @Test
    fun `It should attach the product the recommendation refers to`() {
        // Given
        val product = Product("p1", "Milk", 2.5)
        productInputTopic.pipeInput(product.productId, product)

        // When
        contextInputTopic.pipeInput(product.productId, ProbabilityContext(0.75, "order-1", 3))

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(
            listOf(
                KeyValue(
                    product.productId,
                    ProductWithProbabilityContext(
                        productWithProbability = product.withProbability(0.75),
                        orderId = "order-1",
                        expectedSize = 3,
                    ),
                ),
            ),
        )
    }

    @Test
    fun `It should attach no product when the product is unknown`() {
        // Given
        // When
        contextInputTopic.pipeInput("p2", ProbabilityContext(0.5, "order-2", 1))

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(
            listOf(
                KeyValue(
                    "p2",
                    ProductWithProbabilityContext(
                        productWithProbability = null,
                        orderId = "order-2",
                        expectedSize = 1,
                    ),
                ),
            ),
        )
    }
}
