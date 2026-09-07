package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtProbabilities
import io.github.marcgoosen.groceries.recommender.domain.ProbabilityWithOrder
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-distribution"
private const val OUTPUT_TOPIC = "output-probability-with-order"
class AlsoBoughtProbabilitiesExplodeByProductIdTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, AlsoBoughtProbabilities>
    private lateinit var outputTopic: TestOutputTopic<ProductId, ProbabilityWithOrder>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<OrderId, AlsoBoughtProbabilities>(INPUT_TOPIC)
                .explodeByProductId()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<AlsoBoughtProbabilities>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<ProbabilityWithOrder>().deserializer(),
        )
    }

    @Test
    fun `It should emit one record per product, carrying the order and how many to expect`() {
        // Given
        val distribution = AlsoBoughtProbabilities(mapOf("p1" to 0.6, "p2" to 0.4))

        // When
        inputTopic.pipeInput("o1", distribution)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(
            listOf(
                KeyValue("p1", ProbabilityWithOrder(0.6, "o1", 2)),
                KeyValue("p2", ProbabilityWithOrder(0.4, "o1", 2)),
            ),
        )
    }

    @Test
    fun `It should emit nothing for an empty distribution`() {
        // Given
        val distribution = AlsoBoughtProbabilities()

        // When
        inputTopic.pipeInput("o2", distribution)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(emptyList<KeyValue<ProductId, ProbabilityWithOrder>>())
    }
}
