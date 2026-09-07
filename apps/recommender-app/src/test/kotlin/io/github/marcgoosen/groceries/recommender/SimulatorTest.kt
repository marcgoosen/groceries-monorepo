package io.github.marcgoosen.groceries.recommender

import assertk.assertThat
import assertk.assertions.doesNotContain
import assertk.assertions.each
import assertk.assertions.hasSize
import assertk.assertions.isBetween
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isNotEmpty
import assertk.assertions.isTrue
import assertk.assertions.prop
import io.github.marcgoosen.groceries.recommender.kafkastreams.Topic
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopicNameBuilder
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RoundRobinPartitioner
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

private class DiscardingSerializer<T> : Serializer<T> {
    override fun serialize(topic: String?, data: T?): ByteArray = ByteArray(0)
}

class SimulatorTest {
    private val topics = Topic.entries.associate { it.value to "${it.value}-test" }

    private val productProducer =
        MockProducer<ProductId, Product>(true, RoundRobinPartitioner(), StringSerializer(), DiscardingSerializer())

    private val orderProducer =
        MockProducer<OrderId, Order>(true, RoundRobinPartitioner(), StringSerializer(), DiscardingSerializer())

    private val simulator = Simulator(
        productProducer,
        orderProducer,
        TopicNameBuilder(topics),
        orderInterval = 10.milliseconds,
    )

    @AfterEach
    fun onTearDown() {
        simulator.close()
    }

    @Test
    fun `It should publish the whole catalogue to the product topic on start`() {
        // Given
        // When
        simulator.start()

        val produced = productProducer.history()
        assertThat(produced).hasSize(99)
        assertThat(produced).each { it.transform { record -> record.topic() }.isEqualTo("products-test") }
    }

    @Test
    fun `It should key every product by its own id`() {
        // Given
        // When
        simulator.start()

        assertThat(productProducer.history())
            .each { it.transform { record -> record.key() == record.value().productId }.isTrue() }
    }

    @Test
    fun `It should give every product a distinct id`() {
        // Given
        // When
        simulator.start()

        val ids = productProducer.history().map { it.value().productId }
        assertThat(ids.toSet()).hasSize(ids.size)
    }

    @Test
    fun `It should keep producing orders while it runs`() {
        // Given
        simulator.start()

        // When
        val produced = awaitOrders(3)

        assertThat(produced).isNotEmpty()
        assertThat(produced).each { it.transform { record -> record.topic() }.isEqualTo("orders-test") }
    }

    @Test
    fun `It should order between one and five distinct products at a time`() {
        // Given
        simulator.start()

        // When
        val orders = awaitOrders(5).map { it.value() }

        assertThat(orders).each { order ->
            order.prop(Order::orderLines).transform { lines -> lines.size }.isBetween(1, 5)
            order.prop(Order::orderLines).transform { lines ->
                lines.map { it.productId }.toSet().size == lines.size
            }.isTrue()
        }
    }

    @Test
    fun `It should only order products from its own catalogue`() {
        // Given
        simulator.start()
        val catalogue = productProducer.history().map { it.value().productId }.toSet()

        // When
        val orders = awaitOrders(5).map { it.value() }

        assertThat(orders).each { order ->
            order.prop(Order::orderLines).transform { lines ->
                lines.all { it.productId in catalogue }
            }.isTrue()
        }
    }

    @Test
    fun `It should key every order by its own id`() {
        // Given
        simulator.start()

        // When
        val produced = awaitOrders(3)

        assertThat(produced).each { it.transform { record -> record.key() == record.value().orderId }.isTrue() }
    }

    @Test
    fun `It should give every order a distinct id`() {
        // Given
        simulator.start()

        // When
        val ids = awaitOrders(5).map { it.value().orderId }

        assertThat(ids.toSet()).hasSize(ids.size)
    }

    @Test
    fun `It should stop producing and close its producers when closed`() {
        // Given
        simulator.start()
        awaitOrders(2)

        // When
        simulator.close()

        val afterClose = orderProducer.history().size
        Thread.sleep(100)
        assertThat(orderProducer.history()).hasSize(afterClose)
        assertThat(orderProducer.closed()).isTrue()
        assertThat(productProducer.closed()).isTrue()
    }

    @Test
    fun `It should not have been closed before it is asked to close`() {
        // Given
        // When
        simulator.start()

        assertThat(orderProducer.closed()).isFalse()
    }

    @Test
    fun `It should never order a product it has not published`() {
        // Given
        simulator.start()

        // When
        val orderedProducts = awaitOrders(5).flatMap { it.value().orderLines }.map { it.productId }

        assertThat(orderedProducts).doesNotContain("product-000")
    }

    private fun awaitOrders(count: Int): List<ProducerRecord<OrderId, Order>> {
        val deadline = System.currentTimeMillis() + 5_000
        while (orderProducer.history().size < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(10)
        }
        return orderProducer.history().toList()
    }
}
