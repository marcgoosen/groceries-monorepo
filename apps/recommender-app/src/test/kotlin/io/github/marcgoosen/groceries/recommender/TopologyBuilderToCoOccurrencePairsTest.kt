package io.github.marcgoosen.groceries.recommender

import io.github.marcgoosen.groceries.recommender.kafkastreams.toCoOccurrencePairs
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.ProductId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TopologyBuilderToCoOccurrencePairsTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, Order>
    private lateinit var outputTopic: TestOutputTopic<ProductId, ProductId>

    companion object {
        private const val INPUT_TOPIC = "input-orders"
        private const val OUTPUT_TOPIC = "output"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder.stream<OrderId, Order>(
                INPUT_TOPIC,
            ).toCoOccurrencePairs()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<Order>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.string.deserializer(),
        )
    }

    @Test
    fun `It should work`() {
        val order = faker.order().copy(
            orderLines = listOf(
                faker.orderLine().copy(productId = "product-1"),
                faker.orderLine().copy(productId = "product-2"),
                faker.orderLine().copy(productId = "product-3"),
            ),
        )

        inputTopic.pipeInput(order.orderId, order)
        val result = outputTopic.readKeyValuesToList()
        val expected = listOf(
            KeyValue(
                "product-1",
                "product-2",
            ),
            KeyValue(
                "product-1",
                "product-3",
            ),
            KeyValue(
                "product-2",
                "product-1",
            ),
            KeyValue(
                "product-2",
                "product-3",
            ),
            KeyValue(
                "product-3",
                "product-1",
            ),
            KeyValue(
                "product-3",
                "product-2",
            ),
        )
        result shouldBe expected
    }
}
