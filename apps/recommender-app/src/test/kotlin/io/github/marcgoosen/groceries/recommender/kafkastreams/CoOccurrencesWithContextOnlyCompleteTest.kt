package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrencesWithContext
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.recommender.orderLine
import io.github.marcgoosen.groceries.shared.domain.OrderId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-context"
private const val OUTPUT_TOPIC = "output-complete"

@OptIn(ExperimentalAvro4kApi::class)
class CoOccurrencesWithContextOnlyCompleteTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoOccurrencesWithContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoOccurrencesWithContext>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, CoOccurrencesWithContext>(INPUT_TOPIC)
                .onlyComplete()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<CoOccurrencesWithContext>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<CoOccurrencesWithContext>().deserializer(),
        )
    }

    @Test
    fun `It should drop aggregates that are still missing a product's co-occurrences`() {
        // Given
        val order = faker.order().copy(
            orderId = "o1",
            orderLines = listOf(
                faker.orderLine().copy(productId = "p1"),
                faker.orderLine().copy(productId = "p2"),
            ),
        )
        val complete = CoOccurrencesWithContext(order = order, coOccurrences = listOf(CoOccurrence(), CoOccurrence()))
        val incomplete = CoOccurrencesWithContext(order = order, coOccurrences = listOf(CoOccurrence()))

        // When
        inputTopic.pipeInput(order.orderId, incomplete)
        inputTopic.pipeInput(order.orderId, complete)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(complete))
    }
}
