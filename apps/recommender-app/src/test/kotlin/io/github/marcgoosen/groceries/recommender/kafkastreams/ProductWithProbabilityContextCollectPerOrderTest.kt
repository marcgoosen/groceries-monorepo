package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbabilityContext
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbabilityContext
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-product-context"
private const val OUTPUT_TOPIC = "output-products-collected"
class ProductWithProbabilityContextCollectPerOrderTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<ProductId, ProductWithProbabilityContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, ProductsWithProbabilityContext>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<ProductId, ProductWithProbabilityContext>(INPUT_TOPIC)
                .collectPerOrder()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<ProductWithProbabilityContext>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<ProductsWithProbabilityContext>().deserializer(),
        )
    }

    @Test
    fun `It should gather the recommended products back under the order that asked for them`() {
        // Given
        val milk = ProductWithProbability(Product("p1", "Milk", 2.0), 0.8)
        val bread = ProductWithProbability(Product("p2", "Bread", 1.5), 0.6)

        // When
        inputTopic.pipeInput(milk.product.productId, ProductWithProbabilityContext(milk, "order-1", 2))
        inputTopic.pipeInput(bread.product.productId, ProductWithProbabilityContext(bread, "order-1", 2))

        assertThat(outputTopic.readKeyValuesToMap()["order-1"]).isEqualTo(
            ProductsWithProbabilityContext(
                productsWithProbabilities = listOf(milk, bread),
                expectedSize = 2,
            ),
        )
    }
}
