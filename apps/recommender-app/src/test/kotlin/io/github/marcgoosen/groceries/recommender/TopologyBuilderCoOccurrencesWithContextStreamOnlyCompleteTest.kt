package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrencesWithContext
import io.github.marcgoosen.groceries.recommender.kafkastreams.onlyComplete
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderCoOccurrencesWithContextStreamOnlyCompleteTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoOccurrencesWithContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoOccurrencesWithContext>

    companion object {
        private const val INPUT_TOPIC = "input-context"
        private const val OUTPUT_TOPIC = "output-complete"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<OrderId, CoOccurrencesWithContext>(INPUT_TOPIC)
                .onlyComplete()
                .to(OUTPUT_TOPIC)
        }

        inputTopic =
            topologyTestDriver.createInputTopic(
                INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<CoOccurrencesWithContext>().serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<CoOccurrencesWithContext>().deserializer(),
            )
    }

    @Test
    fun `It should only emit complete CoOccurrencesWithContext`() {
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

        // Complete: 2 productIds, 2 coOccurrences
        val complete =
            CoOccurrencesWithContext(
                order = order,
                coOccurrences = listOf(CoOccurrence(), CoOccurrence()),
            )

        // Incomplete: 2 productIds, 1 coOccurrence
        val incomplete =
            CoOccurrencesWithContext(order = order, coOccurrences = listOf(CoOccurrence()))

        inputTopic.pipeInput(order.orderId, incomplete)
        inputTopic.pipeInput(order.orderId, complete)

        val results = outputTopic.readValuesToList()
        results shouldBe listOf(complete)
    }
}
