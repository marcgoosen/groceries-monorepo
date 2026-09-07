package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.recommender.orderLine
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-orders"
private const val OUTPUT_TOPIC = "output-exploded"

class OrderStreamExplodeByProductIdTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, Order>
    private lateinit var outputTopic: TestOutputTopic<ProductId, Order>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, Order>(INPUT_TOPIC)
                .explodeByProductId()
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
            avroSerdes.create<Order>().deserializer(),
        )
    }

    @Test
    fun `It should emit the whole order once per product it contains`() {
        // Given
        val order = faker.order().copy(
            orderId = "o1",
            orderLines = listOf(
                faker.orderLine().copy(productId = "p1"),
                faker.orderLine().copy(productId = "p2"),
            ),
        )

        // When
        inputTopic.pipeInput(order.orderId, order)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(
            listOf(
                KeyValue("p1", order),
                KeyValue("p2", order),
            ),
        )
    }

    @Test
    fun `It should emit nothing for an order without products`() {
        // Given
        val order = faker.order().copy(orderId = "o2", orderLines = emptyList())

        // When
        inputTopic.pipeInput(order.orderId, order)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(emptyList<KeyValue<ProductId, Order>>())
    }
}
