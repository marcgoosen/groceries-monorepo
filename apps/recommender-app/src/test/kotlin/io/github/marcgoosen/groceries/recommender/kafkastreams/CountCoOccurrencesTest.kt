package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-pairs"
private const val OUTPUT_TOPIC = "output-counts"
class CountCoOccurrencesTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<ProductId, ProductId>
    private lateinit var outputTopic: TestOutputTopic<ProductId, CoOccurrence>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<ProductId, ProductId>(INPUT_TOPIC)
                .countCoOccurrences()
                .toStream()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.string.serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<CoOccurrence>().deserializer(),
        )
    }

    @Test
    fun `It should accumulate a count per co-occurring product`() {
        // Given
        // When
        inputTopic.pipeInput("p1", "p2")
        inputTopic.pipeInput("p1", "p3")
        inputTopic.pipeInput("p1", "p2")

        assertThat(outputTopic.readKeyValuesToMap()["p1"]).isEqualTo(CoOccurrence(mapOf("p2" to 2, "p3" to 1)))
    }

    @Test
    fun `It should keep a separate count per keyed product`() {
        // Given
        // When
        inputTopic.pipeInput("p1", "p2")
        inputTopic.pipeInput("p2", "p1")
        inputTopic.pipeInput("p1", "p2")
        inputTopic.pipeInput("p3", "p1")

        assertThat(outputTopic.readKeyValuesToMap()).isEqualTo(
            mapOf(
                "p1" to CoOccurrence(mapOf("p2" to 2)),
                "p2" to CoOccurrence(mapOf("p1" to 1)),
                "p3" to CoOccurrence(mapOf("p1" to 1)),
            ),
        )
    }
}
