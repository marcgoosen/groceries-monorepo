package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.kafkastreams.countCoOccurrences
import io.github.marcgoosen.groceries.shared.domain.ProductId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderCountCoOccurrencesTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<ProductId, ProductId>
    private lateinit var outputTopic: TestOutputTopic<ProductId, CoOccurrence>

    companion object {
        private const val INPUT_TOPIC = "input-pairs"
        private const val OUTPUT_TOPIC = "output-counts"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<ProductId, ProductId>(INPUT_TOPIC)
                .countCoOccurrences()
                .toStream()
                .to(OUTPUT_TOPIC)
        }

        inputTopic =
            topologyTestDriver.createInputTopic(
                INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.string.serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<CoOccurrence>().deserializer(),
            )
    }

    @Test
    fun `It should count co-occurrences correctly for a single product`() {
        inputTopic.pipeInput("p1", "p2")
        inputTopic.pipeInput("p1", "p3")
        inputTopic.pipeInput("p1", "p2")

        val results = outputTopic.readKeyValuesToMap()

        results["p1"] shouldBe CoOccurrence(mapOf("p2" to 2, "p3" to 1))
    }

    @Test
    fun `It should count co-occurrences correctly for multiple products`() {
        inputTopic.pipeInput("p1", "p2")
        inputTopic.pipeInput("p2", "p1")
        inputTopic.pipeInput("p1", "p2")
        inputTopic.pipeInput("p3", "p1")

        val results = outputTopic.readKeyValuesToMap()

        results["p1"] shouldBe CoOccurrence(mapOf("p2" to 2))
        results["p2"] shouldBe CoOccurrence(mapOf("p1" to 1))
        results["p3"] shouldBe CoOccurrence(mapOf("p1" to 1))
    }
}
