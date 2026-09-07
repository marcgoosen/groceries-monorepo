package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrencesWithContext
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-context"
private const val OUTPUT_TOPIC = "output-rollup"

@OptIn(ExperimentalAvro4kApi::class)
class RollupToCoOccurrenceTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoOccurrencesWithContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoOccurrence>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, CoOccurrencesWithContext>(INPUT_TOPIC)
                .rollupToCoOccurrence()
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
            avroSerdes.create<CoOccurrence>().deserializer(),
        )
    }

    @Test
    fun `It should sum the co-occurrences and drop the products already ordered`() {
        // Given
        val order = faker.order().copy(
            orderId = "o1",
            orderLines = listOf(
                OrderLine("product-1", 10.0, 1),
                OrderLine("product-2", 20.0, 1),
            ),
        )
        val context = CoOccurrencesWithContext(
            order = order,
            coOccurrences = listOf(
                CoOccurrence(mapOf("product-1" to 1, "product-3" to 1)),
                CoOccurrence(mapOf("product-2" to 1, "product-3" to 2)),
            ),
        )

        // When
        inputTopic.pipeInput(order.orderId, context)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(CoOccurrence(mapOf("product-3" to 3))))
    }
}
