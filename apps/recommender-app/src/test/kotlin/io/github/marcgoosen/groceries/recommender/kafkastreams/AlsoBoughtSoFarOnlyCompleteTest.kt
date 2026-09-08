package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCount
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtSoFar
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.recommender.orderLine
import io.github.marcgoosen.groceries.shared.domain.OrderId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-context"
private const val OUTPUT_TOPIC = "output-complete"
class AlsoBoughtSoFarOnlyCompleteTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, AlsoBoughtSoFar>
    private lateinit var outputTopic: TestOutputTopic<OrderId, AlsoBoughtSoFar>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, AlsoBoughtSoFar>(INPUT_TOPIC)
                .onlyComplete()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<AlsoBoughtSoFar>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<AlsoBoughtSoFar>().deserializer(),
        )
    }

    @Test
    fun `It should drop aggregates that are still missing a product's also-bought`() {
        // Given
        val order = faker.order().copy(
            orderId = "o1",
            orderLines = listOf(
                faker.orderLine().copy(productId = "p1"),
                faker.orderLine().copy(productId = "p2"),
            ),
        )
        val complete =
            AlsoBoughtSoFar(
                order = order,
                alsoBought = listOf(AlsoBoughtCount(), AlsoBoughtCount()),
            )
        val incomplete = AlsoBoughtSoFar(order = order, alsoBought = listOf(AlsoBoughtCount()))

        // When
        inputTopic.pipeInput(order.orderId, incomplete)
        inputTopic.pipeInput(order.orderId, complete)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(complete))
    }
}
