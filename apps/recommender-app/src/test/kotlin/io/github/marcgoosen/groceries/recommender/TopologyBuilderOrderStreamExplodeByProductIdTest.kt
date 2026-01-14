package io.github.marcgoosen.groceries.recommender

import io.github.marcgoosen.groceries.recommender.kafkastreams.explodeByProductId
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.ProductId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TopologyBuilderOrderStreamExplodeByProductIdTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, Order>
    private lateinit var outputTopic: TestOutputTopic<ProductId, Order>

    companion object {
        private const val INPUT_TOPIC = "input-orders"
        private const val OUTPUT_TOPIC = "output-exploded"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder.stream<OrderId, Order>(INPUT_TOPIC).explodeByProductId().to(OUTPUT_TOPIC)
        }

        inputTopic =
            topologyTestDriver.createInputTopic(
                INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<Order>().serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<Order>().deserializer(),
            )
    }

    @Test
    fun `It should explode an order into multiple records based on productIds`() {
        val order =
            faker.order()
                .copy(
                    orderId = "o1",
                    orderLines =
                    listOf(
                        faker.orderLine().copy(productId = "p1"),
                        faker.orderLine().copy(productId = "p2"),
                    ),
                )

        inputTopic.pipeInput(order.orderId, order)

        val results = outputTopic.readKeyValuesToList()
        results shouldBe
            listOf(
                KeyValue("p1", order),
                KeyValue("p2", order),
            )
    }

    @Test
    fun `It should not emit anything for an order with no productIds`() {
        val emptyOrder = faker.order().copy(orderId = "o2", orderLines = emptyList())

        inputTopic.pipeInput(emptyOrder.orderId, emptyOrder)

        val results = outputTopic.readKeyValuesToList()
        results shouldBe emptyList<KeyValue<ProductId, Order>>()
    }
}
