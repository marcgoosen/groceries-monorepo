package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoDistribution
import io.github.marcgoosen.groceries.shared.domain.OrderId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-distribution"
private const val OUTPUT_TOPIC = "output-top-n"

@OptIn(ExperimentalAvro4kApi::class)
class SelectTopNTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoDistribution>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoDistribution>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, CoDistribution>(INPUT_TOPIC)
                .selectTopN(2)
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<CoDistribution>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<CoDistribution>().deserializer(),
        )
    }

    @Test
    fun `It should keep only the two most probable products, most probable first`() {
        // Given
        val distribution = CoDistribution(mapOf("p1" to 0.1, "p2" to 0.3, "p3" to 0.5, "p4" to 0.1))

        // When
        inputTopic.pipeInput("o1", distribution)

        assertThat(outputTopic.readValuesToList()).isEqualTo(
            listOf(CoDistribution(mapOf("p3" to 0.5, "p2" to 0.3))),
        )
    }

    @Test
    fun `It should keep every product when there are fewer than N`() {
        // Given
        val distribution = CoDistribution(mapOf("p1" to 0.6, "p2" to 0.4))

        // When
        inputTopic.pipeInput("o2", distribution)

        assertThat(outputTopic.readValuesToList()).isEqualTo(listOf(distribution))
    }
}
