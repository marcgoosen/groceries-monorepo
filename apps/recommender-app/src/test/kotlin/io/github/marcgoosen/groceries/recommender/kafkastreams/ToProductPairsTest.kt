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
private const val OUTPUT_TOPIC = "output-pairs"

class ToProductPairsTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, Order>
    private lateinit var outputTopic: TestOutputTopic<ProductId, ProductId>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, Order>(INPUT_TOPIC)
                .toProductPairs()
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
    fun `It should emit every ordered pair of products in the order`() {
        // Given
        val order = faker.order().copy(
            orderLines = listOf(
                faker.orderLine().copy(productId = "product-1"),
                faker.orderLine().copy(productId = "product-2"),
                faker.orderLine().copy(productId = "product-3"),
            ),
        )

        // When
        inputTopic.pipeInput(order.orderId, order)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(
            listOf(
                KeyValue("product-1", "product-2"),
                KeyValue("product-1", "product-3"),
                KeyValue("product-2", "product-1"),
                KeyValue("product-2", "product-3"),
                KeyValue("product-3", "product-1"),
                KeyValue("product-3", "product-2"),
            ),
        )
    }

    @Test
    fun `It should emit no pairs for an order with a single product`() {
        // Given
        val order = faker.order().copy(orderLines = listOf(faker.orderLine().copy(productId = "product-1")))

        // When
        inputTopic.pipeInput(order.orderId, order)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(emptyList<KeyValue<ProductId, ProductId>>())
    }
}
