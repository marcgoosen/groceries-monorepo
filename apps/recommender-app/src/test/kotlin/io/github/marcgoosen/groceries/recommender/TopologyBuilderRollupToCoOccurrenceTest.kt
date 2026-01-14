package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrencesWithContext
import io.github.marcgoosen.groceries.recommender.kafkastreams.rollupToCoOccurrence
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderRollupToCoOccurrenceTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoOccurrencesWithContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoOccurrence>

    companion object {
        private const val INPUT_TOPIC = "input-context"
        private const val OUTPUT_TOPIC = "output-rollup"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<OrderId, CoOccurrencesWithContext>(INPUT_TOPIC)
                .rollupToCoOccurrence()
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
                avroSerdes.create<CoOccurrence>().deserializer(),
            )
    }

    @Test
    fun `It should rollup co-occurrences and remove productIds already in the order`() {
        val p1 = "product-1"
        val p2 = "product-2"
        val p3 = "product-3"

        val order =
            faker.order()
                .copy(
                    orderId = "o1",
                    orderLines =
                    listOf(
                        OrderLine(p1, 10.0, 1),
                        OrderLine(p2, 20.0, 1),
                    ),
                )

        // Co-occurrences to rollup
        val co1 = CoOccurrence(mapOf(p1 to 1, p3 to 1))
        val co2 = CoOccurrence(mapOf(p2 to 1, p3 to 2))

        val context =
            CoOccurrencesWithContext(
                order = order,
                coOccurrences = listOf(co1, co2),
            )

        inputTopic.pipeInput(order.orderId, context)

        val results = outputTopic.readValuesToList()

        // Rollup (co1 + co2) = {p1: 1, p2: 1, p3: 3}
        // Subtract order productIds {p1, p2}
        // Expected: {p3: 3}
        results shouldBe listOf(CoOccurrence(mapOf(p3 to 3)))
    }
}
