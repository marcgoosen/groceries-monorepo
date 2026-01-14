package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.ProbabilityContext
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbabilityContext
import io.github.marcgoosen.groceries.recommender.kafkastreams.joinWithProduct
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderJoinWithProductTest : BaseTopologyBuilderTest() {
    private lateinit var contextInputTopic: TestInputTopic<ProductId, ProbabilityContext>
    private lateinit var productInputTopic: TestInputTopic<ProductId, Product>
    private lateinit var outputTopic: TestOutputTopic<ProductId, ProductWithProbabilityContext>

    companion object {
        private const val CONTEXT_INPUT_TOPIC = "input-context"
        private const val PRODUCT_INPUT_TOPIC = "input-products"
        private const val OUTPUT_TOPIC = "output-joined"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            val productTable = streamsBuilder.table<ProductId, Product>(PRODUCT_INPUT_TOPIC)

            streamsBuilder
                .stream<ProductId, ProbabilityContext>(CONTEXT_INPUT_TOPIC)
                .joinWithProduct(productTable)
                .to(OUTPUT_TOPIC)
        }

        contextInputTopic =
            topologyTestDriver.createInputTopic(
                CONTEXT_INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<ProbabilityContext>().serializer(),
            )

        productInputTopic =
            topologyTestDriver.createInputTopic(
                PRODUCT_INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<Product>().serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<ProductWithProbabilityContext>().deserializer(),
            )
    }

    @Test
    fun `It should join probability context with existing product data`() {
        val productId = "p1"
        val context = ProbabilityContext(0.75, "order-1", 3)
        val product = Product(productId, "Milk", 2.5)

        productInputTopic.pipeInput(productId, product)
        contextInputTopic.pipeInput(productId, context)

        val results = outputTopic.readKeyValuesToList()
        results shouldBe
            listOf(
                KeyValue(
                    productId,
                    ProductWithProbabilityContext(
                        productWithProbability = product.withProbability(0.75),
                        orderId = "order-1",
                        expectedSize = 3,
                    ),
                ),
            )
    }

    @Test
    fun `It should join with null product when product data is missing`() {
        val productId = "p2"
        val context = ProbabilityContext(0.5, "order-2", 1)

        contextInputTopic.pipeInput(productId, context)

        val results = outputTopic.readKeyValuesToList()
        results shouldBe
            listOf(
                KeyValue(
                    productId,
                    ProductWithProbabilityContext(
                        productWithProbability = null,
                        orderId = "order-2",
                        expectedSize = 1,
                    ),
                ),
            )
    }
}

private fun Product.withProbability(probability: Double) = ProductWithProbability(this, probability)
