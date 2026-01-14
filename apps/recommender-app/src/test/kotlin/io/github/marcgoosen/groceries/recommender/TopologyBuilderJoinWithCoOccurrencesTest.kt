package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrenceWithContext
import io.github.marcgoosen.groceries.recommender.kafkastreams.joinWithCoOccurrences
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.ProductId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderJoinWithCoOccurrencesTest : BaseTopologyBuilderTest() {
    private lateinit var orderInputTopic: TestInputTopic<ProductId, Order>
    private lateinit var coOccurrenceInputTopic: TestInputTopic<ProductId, CoOccurrence>
    private lateinit var outputTopic: TestOutputTopic<ProductId, CoOccurrenceWithContext>

    companion object {
        private const val ORDER_INPUT_TOPIC = "input-orders"
        private const val CO_OCCURRENCE_INPUT_TOPIC = "input-co-occurrences"
        private const val OUTPUT_TOPIC = "output-joined"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            val coOccurrenceTable =
                streamsBuilder.table<ProductId, CoOccurrence>(
                    CO_OCCURRENCE_INPUT_TOPIC,
                )

            streamsBuilder
                .stream<ProductId, Order>(ORDER_INPUT_TOPIC)
                .joinWithCoOccurrences(coOccurrenceTable)
                .to(OUTPUT_TOPIC)
        }

        orderInputTopic =
            topologyTestDriver.createInputTopic(
                ORDER_INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<Order>().serializer(),
            )

        coOccurrenceInputTopic =
            topologyTestDriver.createInputTopic(
                CO_OCCURRENCE_INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<CoOccurrence>().serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<CoOccurrenceWithContext>().deserializer(),
            )
    }

    @Test
    fun `It should join order with existing co-occurrence data`() {
        val productId = "p1"
        val order = faker.order().copy(orderId = "o1")
        val coOccurrence = CoOccurrence(mapOf("p2" to 5))

        // Populate table
        coOccurrenceInputTopic.pipeInput(productId, coOccurrence)

        // Process stream
        orderInputTopic.pipeInput(productId, order)

        val results = outputTopic.readKeyValuesToList()
        results shouldBe listOf(KeyValue(productId, CoOccurrenceWithContext(coOccurrence, order)))
    }

    @Test
    fun `It should join order with empty co-occurrence when data is missing in table`() {
        val productId = "p1"
        val order = faker.order().copy(orderId = "o1")

        // No data in table
        orderInputTopic.pipeInput(productId, order)

        val results = outputTopic.readKeyValuesToList()
        results shouldBe listOf(KeyValue(productId, CoOccurrenceWithContext(CoOccurrence(), order)))
    }
}
