package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCount
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtProbabilities
import io.github.marcgoosen.groceries.shared.domain.OrderId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-also-bought"
private const val OUTPUT_TOPIC = "output-distribution"
class ToAlsoBoughtProbabilitiesTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, AlsoBoughtCount>
    private lateinit var outputTopic: TestOutputTopic<OrderId, AlsoBoughtProbabilities>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, AlsoBoughtCount>(INPUT_TOPIC)
                .toAlsoBoughtProbabilities()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<AlsoBoughtCount>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<AlsoBoughtProbabilities>().deserializer(),
        )
    }

    @Test
    fun `It should turn counts into each product's share of the total`() {
        // Given
        val alsoBought = AlsoBoughtCount(mapOf("p1" to 2, "p2" to 3))

        // When
        inputTopic.pipeInput("o1", alsoBought)

        assertThat(outputTopic.readValuesToList()).isEqualTo(
            listOf(AlsoBoughtProbabilities(mapOf("p1" to 0.4, "p2" to 0.6))),
        )
    }

    @Test
    fun `It should emit an empty distribution when nothing co-occurred`() {
        // Given
        val alsoBought = AlsoBoughtCount()

        // When
        inputTopic.pipeInput("o2", alsoBought)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(AlsoBoughtProbabilities()))
    }
}
