package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.CoDistribution
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.shared.domain.OrderId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-co-occurrence"
private const val OUTPUT_TOPIC = "output-distribution"
class ToCoDistributionTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoOccurrence>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoDistribution>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, CoOccurrence>(INPUT_TOPIC)
                .toCoDistribution()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<CoOccurrence>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<CoDistribution>().deserializer(),
        )
    }

    @Test
    fun `It should turn counts into each product's share of the total`() {
        // Given
        val coOccurrence = CoOccurrence(mapOf("p1" to 2, "p2" to 3))

        // When
        inputTopic.pipeInput("o1", coOccurrence)

        assertThat(outputTopic.readValuesToList()).isEqualTo(
            listOf(CoDistribution(mapOf("p1" to 0.4, "p2" to 0.6))),
        )
    }

    @Test
    fun `It should emit an empty distribution when nothing co-occurred`() {
        // Given
        val coOccurrence = CoOccurrence()

        // When
        inputTopic.pipeInput("o2", coOccurrence)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(CoDistribution()))
    }
}
