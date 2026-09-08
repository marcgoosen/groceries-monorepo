package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCount
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtSoFar
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-context"
private const val OUTPUT_TOPIC = "output-sum"
class SumAlsoBoughtTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, AlsoBoughtSoFar>
    private lateinit var outputTopic: TestOutputTopic<OrderId, AlsoBoughtCount>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, AlsoBoughtSoFar>(INPUT_TOPIC)
                .sumAlsoBought()
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
            avroSerdes.create<AlsoBoughtCount>().deserializer(),
        )
    }

    @Test
    fun `It should sum the also-bought and drop the products already ordered`() {
        // Given
        val order = faker.order().copy(
            orderId = "o1",
            orderLines = listOf(
                OrderLine("product-1", 10.0, 1),
                OrderLine("product-2", 20.0, 1),
            ),
        )
        val context = AlsoBoughtSoFar(
            order = order,
            alsoBought = listOf(
                AlsoBoughtCount(mapOf("product-1" to 1, "product-3" to 1)),
                AlsoBoughtCount(mapOf("product-2" to 1, "product-3" to 2)),
            ),
        )

        // When
        inputTopic.pipeInput(order.orderId, context)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(AlsoBoughtCount(mapOf("product-3" to 3))))
    }
}
