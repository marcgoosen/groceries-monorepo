package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbabilityContext
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbabilityContext
import io.github.marcgoosen.groceries.recommender.kafkastreams.collectPerOrder
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderProductsCollectPerOrderTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<ProductId, ProductWithProbabilityContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, ProductsWithProbabilityContext>

    companion object {
        private const val INPUT_TOPIC = "input-product-context"
        private const val OUTPUT_TOPIC = "output-products-collected"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<ProductId, ProductWithProbabilityContext>(INPUT_TOPIC)
                .collectPerOrder()
                .to(OUTPUT_TOPIC)
        }

        inputTopic =
            topologyTestDriver.createInputTopic(
                INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<ProductWithProbabilityContext>().serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<ProductsWithProbabilityContext>().deserializer(),
            )
    }

    @Test
    fun `It should aggregate individual product contexts by orderId`() {
        val orderId = "order-1"
        val p1 = Product("p1", "Milk", 2.0)
        val p2 = Product("p2", "Bread", 1.5)

        val pwp1 = ProductWithProbability(p1, 0.8)
        val pwp2 = ProductWithProbability(p2, 0.6)

        val ctx1 = ProductWithProbabilityContext(pwp1, orderId, 2)
        val ctx2 = ProductWithProbabilityContext(pwp2, orderId, 2)

        inputTopic.pipeInput(p1.productId, ctx1)
        inputTopic.pipeInput(p2.productId, ctx2)

        val results = outputTopic.readKeyValuesToMap()

        results[orderId] shouldBe
            ProductsWithProbabilityContext(
                productsWithProbabilities = listOf(pwp1, pwp2),
                expectedSize = 2,
            )
    }
}
