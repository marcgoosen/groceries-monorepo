package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoDistribution
import io.github.marcgoosen.groceries.recommender.domain.ProbabilityContext
import io.github.marcgoosen.groceries.recommender.kafkastreams.explodeByProductId
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.ProductId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderCoDistributionStreamExplodeByProductIdTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<OrderId, CoDistribution>
    private lateinit var outputTopic: TestOutputTopic<ProductId, ProbabilityContext>

    companion object {
        private const val INPUT_TOPIC = "input-distribution"
        private const val OUTPUT_TOPIC = "output-probability-context"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<OrderId, CoDistribution>(INPUT_TOPIC)
                .explodeByProductId()
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
                avroSerdes.create<ProbabilityContext>().deserializer(),
            )
    }

    @Test
    fun `It should explode a distribution into multiple records keyed by productId`() {
        val orderId = "o1"
        val dist = CoDistribution(mapOf("p1" to 0.6, "p2" to 0.4))

        inputTopic.pipeInput(orderId, dist)

        val results = outputTopic.readKeyValuesToList()
        results shouldBe
            listOf(
                KeyValue("p1", ProbabilityContext(0.6, orderId, 2)),
                KeyValue("p2", ProbabilityContext(0.4, orderId, 2)),
            )
    }

    @Test
    fun `It should not emit anything for an empty distribution`() {
        val orderId = "o2"
        val dist = CoDistribution(emptyMap())

        inputTopic.pipeInput(orderId, dist)

        val results = outputTopic.readKeyValuesToList()
        results shouldBe emptyList<KeyValue<ProductId, ProbabilityContext>>()
    }
}
