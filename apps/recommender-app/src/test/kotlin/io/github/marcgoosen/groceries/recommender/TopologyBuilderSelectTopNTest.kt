package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoDistribution
import io.github.marcgoosen.groceries.recommender.kafkastreams.selectTopN
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderSelectTopNTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoDistribution>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoDistribution>

    companion object {
        private const val INPUT_TOPIC = "input-distribution"
        private const val OUTPUT_TOPIC = "output-top-n"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<OrderId, CoDistribution>(INPUT_TOPIC)
                .selectTopN(2)
                .to(OUTPUT_TOPIC)
        }

        inputTopic =
            topologyTestDriver.createInputTopic(
                INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<CoDistribution>().serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<CoDistribution>().deserializer(),
            )
    }

    @Test
    fun `It should select the top N products based on probability`() {
        val orderId = "o1"
        val dist = CoDistribution(mapOf("p1" to 0.1, "p2" to 0.3, "p3" to 0.5, "p4" to 0.1))

        inputTopic.pipeInput(orderId, dist)

        val results = outputTopic.readValuesToList()

        // n=2, so should take p2 (0.5) and p3 (0.3)
        results shouldBe listOf(CoDistribution(mapOf("p3" to 0.5, "p2" to 0.3)))
    }

    @Test
    fun `It should return all products if N is greater than the distribution size`() {
        val orderId = "o2"
        val dist = CoDistribution(mapOf("p1" to 0.6, "p2" to 0.4))

        inputTopic.pipeInput(orderId, dist)

        val results = outputTopic.readValuesToList()

        // n=2, size=2, should return both
        results shouldBe listOf(dist)
    }
}
