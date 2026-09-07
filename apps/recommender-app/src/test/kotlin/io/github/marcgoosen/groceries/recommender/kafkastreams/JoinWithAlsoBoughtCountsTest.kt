package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCount
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCountWithOrder
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val ORDER_INPUT_TOPIC = "input-orders"
private const val ALSO_BOUGHT_INPUT_TOPIC = "input-also-bought"
private const val OUTPUT_TOPIC = "output-joined"
class JoinWithAlsoBoughtCountsTest : BaseTopologyTest() {
    private lateinit var orderInputTopic: TestInputTopic<ProductId, Order>
    private lateinit var alsoBoughtInputTopic: TestInputTopic<ProductId, AlsoBoughtCount>
    private lateinit var outputTopic: TestOutputTopic<ProductId, AlsoBoughtCountWithOrder>

    @BeforeEach
    fun onSetup() {
        setup {
            val alsoBoughtTable = streamsBuilder.table<ProductId, AlsoBoughtCount>(ALSO_BOUGHT_INPUT_TOPIC)

            streamsBuilder.stream<ProductId, Order>(ORDER_INPUT_TOPIC)
                .joinWithAlsoBoughtCounts(alsoBoughtTable)
                .to(OUTPUT_TOPIC)
        }

        orderInputTopic = topologyTestDriver.createInputTopic(
            ORDER_INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<Order>().serializer(),
        )

        alsoBoughtInputTopic = topologyTestDriver.createInputTopic(
            ALSO_BOUGHT_INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<AlsoBoughtCount>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<AlsoBoughtCountWithOrder>().deserializer(),
        )
    }

    @Test
    fun `It should attach the also-bought counts known for the product`() {
        // Given
        val order = faker.order().copy(orderId = "o1")
        val alsoBought = AlsoBoughtCount(mapOf("p2" to 5))
        alsoBoughtInputTopic.pipeInput("p1", alsoBought)

        // When
        orderInputTopic.pipeInput("p1", order)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(
            listOf(KeyValue("p1", AlsoBoughtCountWithOrder(alsoBought, order))),
        )
    }

    @Test
    fun `It should attach an empty also-bought for a product not yet in the table`() {
        // Given
        val order = faker.order().copy(orderId = "o1")

        // When
        orderInputTopic.pipeInput("p1", order)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(
            listOf(KeyValue("p1", AlsoBoughtCountWithOrder(AlsoBoughtCount(), order))),
        )
    }
}
