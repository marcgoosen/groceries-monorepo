package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.RelatedProduct
import io.github.marcgoosen.groceries.recommender.domain.RelatedProductWithOrder
import io.github.marcgoosen.groceries.recommender.domain.RelatedProductsSoFar
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-product-context"
private const val OUTPUT_TOPIC = "output-products-collected"
class RelatedProductWithOrderCollectPerOrderTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<ProductId, RelatedProductWithOrder>
    private lateinit var outputTopic: TestOutputTopic<OrderId, RelatedProductsSoFar>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<ProductId, RelatedProductWithOrder>(INPUT_TOPIC)
                .collectPerOrder()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<RelatedProductWithOrder>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<RelatedProductsSoFar>().deserializer(),
        )
    }

    @Test
    fun `It should gather the recommended products back under the order that asked for them`() {
        // Given
        val milk = RelatedProduct(Product("p1", "Milk", 2.0), 0.8)
        val bread = RelatedProduct(Product("p2", "Bread", 1.5), 0.6)

        // When
        inputTopic.pipeInput(milk.product.productId, RelatedProductWithOrder(milk, "order-1", 2))
        inputTopic.pipeInput(bread.product.productId, RelatedProductWithOrder(bread, "order-1", 2))

        assertThat(outputTopic.readKeyValuesToMap()["order-1"]).isEqualTo(
            RelatedProductsSoFar(
                relatedProducts = listOf(milk, bread),
                expectedSize = 2,
            ),
        )
    }
}
