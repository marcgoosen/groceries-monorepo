package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrenceWithContext
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrencesWithContext
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-context"
private const val OUTPUT_TOPIC = "output-collected"

@OptIn(ExperimentalAvro4kApi::class)
class CoOccurrenceWithContextCollectPerOrderTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoOccurrenceWithContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoOccurrencesWithContext>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, CoOccurrenceWithContext>(INPUT_TOPIC)
                .collectPerOrder()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<CoOccurrenceWithContext>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<CoOccurrencesWithContext>().deserializer(),
        )
    }

    @Test
    fun `It should gather every product's co-occurrences under the order they belong to`() {
        // Given
        val order = faker.order().copy(orderId = "order-1")
        val first = CoOccurrenceWithContext(CoOccurrence(mapOf("p1" to 1)), order)
        val second = CoOccurrenceWithContext(CoOccurrence(mapOf("p2" to 1)), order)

        // When
        inputTopic.pipeInput(order.orderId, first)
        inputTopic.pipeInput(order.orderId, second)

        assertThat(outputTopic.readKeyValuesToMap()[order.orderId]).isEqualTo(
            CoOccurrencesWithContext(
                order = order,
                coOccurrences = listOf(first.coOccurrence, second.coOccurrence),
            ),
        )
    }
}
