package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrenceWithContext
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val ORDER_INPUT_TOPIC = "input-orders"
private const val CO_OCCURRENCE_INPUT_TOPIC = "input-co-occurrences"
private const val OUTPUT_TOPIC = "output-joined"

@OptIn(ExperimentalAvro4kApi::class)
class JoinWithCoOccurrencesTest : BaseTopologyTest() {
    private lateinit var orderInputTopic: TestInputTopic<ProductId, Order>
    private lateinit var coOccurrenceInputTopic: TestInputTopic<ProductId, CoOccurrence>
    private lateinit var outputTopic: TestOutputTopic<ProductId, CoOccurrenceWithContext>

    @BeforeEach
    fun onSetup() {
        setup {
            val coOccurrenceTable = streamsBuilder.table<ProductId, CoOccurrence>(CO_OCCURRENCE_INPUT_TOPIC)

            streamsBuilder.stream<ProductId, Order>(ORDER_INPUT_TOPIC)
                .joinWithCoOccurrences(coOccurrenceTable)
                .to(OUTPUT_TOPIC)
        }

        orderInputTopic = topologyTestDriver.createInputTopic(
            ORDER_INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<Order>().serializer(),
        )

        coOccurrenceInputTopic = topologyTestDriver.createInputTopic(
            CO_OCCURRENCE_INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<CoOccurrence>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<CoOccurrenceWithContext>().deserializer(),
        )
    }

    @Test
    fun `It should attach the co-occurrence counts known for the product`() {
        // Given
        val order = faker.order().copy(orderId = "o1")
        val coOccurrence = CoOccurrence(mapOf("p2" to 5))
        coOccurrenceInputTopic.pipeInput("p1", coOccurrence)

        // When
        orderInputTopic.pipeInput("p1", order)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(
            listOf(KeyValue("p1", CoOccurrenceWithContext(coOccurrence, order))),
        )
    }

    @Test
    fun `It should attach an empty co-occurrence for a product not yet in the table`() {
        // Given
        val order = faker.order().copy(orderId = "o1")

        // When
        orderInputTopic.pipeInput("p1", order)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(
            listOf(KeyValue("p1", CoOccurrenceWithContext(CoOccurrence(), order))),
        )
    }
}
