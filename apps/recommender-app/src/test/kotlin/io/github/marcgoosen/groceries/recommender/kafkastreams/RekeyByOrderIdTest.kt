package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCount
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCountWithOrder
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val INPUT_TOPIC = "input-context"
private const val OUTPUT_TOPIC = "output-rekeyed"
class RekeyByOrderIdTest : BaseTopologyTest() {
    private lateinit var inputTopic: TestInputTopic<ProductId, AlsoBoughtCountWithOrder>
    private lateinit var outputTopic: TestOutputTopic<OrderId, AlsoBoughtCountWithOrder>

    @BeforeEach
    fun onSetup() {
        setup {
            streamsBuilder.stream<ProductId, AlsoBoughtCountWithOrder>(INPUT_TOPIC)
                .rekeyByOrderId()
                .to(OUTPUT_TOPIC)
        }

        inputTopic = topologyTestDriver.createInputTopic(
            INPUT_TOPIC,
            avroSerdes.string.serializer(),
            avroSerdes.create<AlsoBoughtCountWithOrder>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            OUTPUT_TOPIC,
            avroSerdes.string.deserializer(),
            avroSerdes.create<AlsoBoughtCountWithOrder>().deserializer(),
        )
    }

    @Test
    fun `It should replace the product key with the order it came from`() {
        // Given
        val order = faker.order().copy(orderId = "order-123")
        val context = AlsoBoughtCountWithOrder(AlsoBoughtCount(), order)

        // When
        inputTopic.pipeInput("product-456", context)

        assertThat(outputTopic.readKeyValuesToList()).isEqualTo(listOf(KeyValue("order-123", context)))
    }
}
