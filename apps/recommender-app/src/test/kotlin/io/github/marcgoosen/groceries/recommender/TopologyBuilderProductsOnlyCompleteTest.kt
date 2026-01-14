package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbabilityContext
import io.github.marcgoosen.groceries.recommender.kafkastreams.onlyComplete
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderProductsOnlyCompleteTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, ProductsWithProbabilityContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, ProductsWithProbabilityContext>

    companion object {
        private const val INPUT_TOPIC = "input-products-context"
        private const val OUTPUT_TOPIC = "output-products-complete"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<OrderId, ProductsWithProbabilityContext>(INPUT_TOPIC)
                .onlyComplete()
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
                avroSerdes.create<ProductsWithProbabilityContext>().deserializer(),
            )
    }

    @Test
    fun `It should only emit complete ProductsWithProbabilityContext`() {
        val p1 = Product("p1", "Milk", 2.0)
        val pwp1 = ProductWithProbability(p1, 0.8)

        // Complete: expectedSize 1, has 1 product
        val complete =
            ProductsWithProbabilityContext(
                productsWithProbabilities = listOf(pwp1),
                expectedSize = 1,
            )

        // Incomplete: expectedSize 2, has 1 product
        val incomplete =
            ProductsWithProbabilityContext(
                productsWithProbabilities = listOf(pwp1),
                expectedSize = 2,
            )

        inputTopic.pipeInput("o1", incomplete)
        inputTopic.pipeInput("o1", complete)

        val results = outputTopic.readValuesToList()
        results shouldBe listOf(complete)
    }
}
