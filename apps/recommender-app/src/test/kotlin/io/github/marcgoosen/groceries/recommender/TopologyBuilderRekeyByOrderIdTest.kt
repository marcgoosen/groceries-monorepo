package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrenceWithContext
import io.github.marcgoosen.groceries.recommender.kafkastreams.rekeyByOrderId
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.ProductId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderRekeyByOrderIdTest : BaseTopologyBuilderTest() {
    private lateinit var inputTopic: TestInputTopic<ProductId, CoOccurrenceWithContext>
    private lateinit var outputTopic: TestOutputTopic<OrderId, CoOccurrenceWithContext>

    companion object {
        private const val INPUT_TOPIC = "input-context"
        private const val OUTPUT_TOPIC = "output-rekeyed"
    }

    @BeforeEach
    fun setup() {
        super.setup {
            streamsBuilder
                .stream<ProductId, CoOccurrenceWithContext>(INPUT_TOPIC)
                .rekeyByOrderId()
                .to(OUTPUT_TOPIC)
        }

        inputTopic =
            topologyTestDriver.createInputTopic(
                INPUT_TOPIC,
                avroSerdes.string.serializer(),
                avroSerdes.create<CoOccurrenceWithContext>().serializer(),
            )

        outputTopic =
            topologyTestDriver.createOutputTopic(
                OUTPUT_TOPIC,
                avroSerdes.string.deserializer(),
                avroSerdes.create<CoOccurrenceWithContext>().deserializer(),
            )
    }

    @Test
    fun `It should rekey records from productId to orderId`() {
        val order = faker.order().copy(orderId = "order-123")
        val context = CoOccurrenceWithContext(CoOccurrence(), order)

        inputTopic.pipeInput("product-456", context)

        val results = outputTopic.readKeyValuesToList()
        results shouldBe listOf(KeyValue("order-123", context))
    }
}
