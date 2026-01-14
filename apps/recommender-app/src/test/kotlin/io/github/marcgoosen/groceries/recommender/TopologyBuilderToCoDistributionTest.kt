package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoDistribution
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.kafkastreams.toCoDistribution
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderToCoDistributionTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoOccurrence>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoDistribution>

    companion object {
        private const val INPUT_TOPIC = "input-co-occurrence"
        private const val OUTPUT_TOPIC = "output-distribution"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<OrderId, CoOccurrence>(INPUT_TOPIC)
                .toCoDistribution()
                .to(OUTPUT_TOPIC)
        }

        inputTopic =
            topologyTestDriver.createInputTopic(
                INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<CoOccurrence>().serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<CoDistribution>().deserializer(),
            )
    }

    @Test
    fun `It should convert co-occurrence counts to probabilities`() {
        val orderId = "o1"
        val co = CoOccurrence(mapOf("p1" to 2, "p2" to 3))

        inputTopic.pipeInput(orderId, co)

        val results = outputTopic.readValuesToList()

        // totalCount = 5
        // p1 = 2/5 = 0.4
        // p2 = 3/5 = 0.6
        results shouldBe listOf(CoDistribution(mapOf("p1" to 0.4, "p2" to 0.6)))
    }

    @Test
    fun `It should return an empty distribution for an empty co-occurrence`() {
        val orderId = "o2"
        val co = CoOccurrence(emptyMap())

        inputTopic.pipeInput(orderId, co)

        val results = outputTopic.readValuesToList()
        results shouldBe listOf(CoDistribution(emptyMap()))
    }
}
