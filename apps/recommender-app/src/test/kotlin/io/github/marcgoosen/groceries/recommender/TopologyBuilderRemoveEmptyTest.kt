package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbabilityContext
import io.github.marcgoosen.groceries.recommender.kafkastreams.removeEmpty
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderRemoveEmptyTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, ProductsWithProbabilityContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, ProductsWithProbability>

    companion object {
        private const val INPUT_TOPIC = "input-context"
        private const val OUTPUT_TOPIC = "output-products"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<OrderId, ProductsWithProbabilityContext>(INPUT_TOPIC)
                .removeEmpty()
                .to(OUTPUT_TOPIC)
        }

        inputTopic =
            topologyTestDriver.createInputTopic(
                INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<ProductsWithProbabilityContext>().serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<ProductsWithProbability>().deserializer(),
            )
    }

    @Test
    fun `It should remove null products from the collection`() {
        val orderId = "o1"
        val p1 = Product("p1", "Milk", 2.0)
        val pwp1 = ProductWithProbability(p1, 0.8)

        val context =
            ProductsWithProbabilityContext(
                productsWithProbabilities = listOf(pwp1, null),
                expectedSize = 2,
            )

        inputTopic.pipeInput(orderId, context)

        val results = outputTopic.readValuesToList()
        results shouldBe listOf(ProductsWithProbability(listOf(pwp1)))
    }

    @Test
    fun `It should return empty ProductsWithProbability if all products are null`() {
        val orderId = "o2"
        val context =
            ProductsWithProbabilityContext(
                productsWithProbabilities = listOf(null, null),
                expectedSize = 2,
            )

        inputTopic.pipeInput(orderId, context)

        val results = outputTopic.readValuesToList()
        results shouldBe listOf(ProductsWithProbability(emptyList()))
    }
}
