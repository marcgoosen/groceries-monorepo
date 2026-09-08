package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCount
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCountWithOrder
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtSoFar
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-context"
private const val OUTPUT_TOPIC = "output-collected"
class AlsoBoughtCountWithOrderCollectPerOrderTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, AlsoBoughtCountWithOrder>
    private lateinit var outputTopic: TestOutputTopic<OrderId, AlsoBoughtSoFar>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, AlsoBoughtCountWithOrder>(INPUT_TOPIC)
                .collectPerOrder()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<AlsoBoughtCountWithOrder>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<AlsoBoughtSoFar>().deserializer(),
        )
    }

    @Test
    fun `It should gather every product's also-bought under the order they belong to`() {
        // Given
        val order = faker.order().copy(orderId = "order-1")
        val first = AlsoBoughtCountWithOrder(AlsoBoughtCount(mapOf("p1" to 1)), order)
        val second = AlsoBoughtCountWithOrder(AlsoBoughtCount(mapOf("p2" to 1)), order)

        // When
        inputTopic.pipeInput(order.orderId, first)
        inputTopic.pipeInput(order.orderId, second)

        assertThat(outputTopic.readKeyValuesToMap()[order.orderId]).isEqualTo(
            AlsoBoughtSoFar(
                order = order,
                alsoBought = listOf(first.alsoBought, second.alsoBought),
            ),
        )
    }
}
