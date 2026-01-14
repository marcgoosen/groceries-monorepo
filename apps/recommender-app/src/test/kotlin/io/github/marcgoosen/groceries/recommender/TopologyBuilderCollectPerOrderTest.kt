package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrenceWithContext
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrencesWithContext
import io.github.marcgoosen.groceries.recommender.kafkastreams.collectPerOrder
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderCollectPerOrderTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoOccurrenceWithContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoOccurrencesWithContext>

    companion object {
        private const val INPUT_TOPIC = "input-context"
        private const val OUTPUT_TOPIC = "output-collected"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<OrderId, CoOccurrenceWithContext>(INPUT_TOPIC)
                .collectPerOrder()
                .to(OUTPUT_TOPIC)
        }

        inputTopic =
            topologyTestDriver.createInputTopic(
                INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<CoOccurrenceWithContext>().serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<CoOccurrencesWithContext>().deserializer(),
            )
    }

    @Test
    fun `It should aggregate individual co-occurrence contexts by orderId`() {
        val order = faker.order().copy(orderId = "order-1")
        val ctx1 = CoOccurrenceWithContext(CoOccurrence(mapOf("p1" to 1)), order)
        val ctx2 = CoOccurrenceWithContext(CoOccurrence(mapOf("p2" to 1)), order)

        inputTopic.pipeInput(order.orderId, ctx1)
        inputTopic.pipeInput(order.orderId, ctx2)

        val results = outputTopic.readKeyValuesToMap()

        results[order.orderId] shouldBe
            CoOccurrencesWithContext(
                order = order,
                coOccurrences = listOf(ctx1.coOccurrence, ctx2.coOccurrence),
            )
    }
}
