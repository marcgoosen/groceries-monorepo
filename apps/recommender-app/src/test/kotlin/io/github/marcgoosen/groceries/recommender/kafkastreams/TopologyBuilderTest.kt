package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbability
import io.github.marcgoosen.groceries.recommender.order
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderTest : BaseTopologyTest() {
    private lateinit var orderInputTopic: TestInputTopic<OrderId, Order>
    private lateinit var productInputTopic: TestInputTopic<ProductId, Product>
    private lateinit var outputTopic: TestOutputTopic<OrderId, ProductsWithProbability>

    private val milk = Product("p1", "Milk", 2.0)
    private val bread = Product("p2", "Bread", 1.5)
    private val eggs = Product("p3", "Eggs", 3.0)
    private val cheese = Product("p4", "Cheese", 4.0)
    private val saffron = Product("p5", "Saffron", 9.0)

    @BeforeEach
    fun onSetup() {
        setup { build() }

        orderInputTopic = topologyTestDriver.createInputTopic(
            Topic.ORDER.topicName,
            avroSerdes.string.serializer(),
            avroSerdes.create<Order>().serializer(),
        )

        productInputTopic = topologyTestDriver.createInputTopic(
            Topic.PRODUCT.topicName,
            avroSerdes.string.serializer(),
            avroSerdes.create<Product>().serializer(),
        )

        outputTopic = topologyTestDriver.createOutputTopic(
            Topic.RELATED_PRODUCTS.topicName,
            avroSerdes.string.deserializer(),
            avroSerdes.create<ProductsWithProbability>().deserializer(),
        )

        listOf(milk, bread, eggs, cheese, saffron).forEach { productInputTopic.pipeInput(it.productId, it) }
    }

    @Test
    fun `It should recommend the products bought alongside the ordered one, and not the ordered one itself`() {
        // Given
        pipeOrder("training-1", milk, bread, eggs)

        // When
        pipeOrder("target-1", milk)

        assertThat(recommendationsFor("target-1")).isEqualTo(
            setOf(
                ProductWithProbability(bread, 0.5),
                ProductWithProbability(eggs, 0.5),
            ),
        )
    }

    @Test
    fun `It should recommend at most the three most likely products`() {
        // Given
        repeat(3) { pipeOrder("skew-1-$it", milk, bread, eggs, cheese) }
        pipeOrder("skew-2", milk, saffron)

        // When
        pipeOrder("target-skew", milk)

        assertThat(recommendationsFor("target-skew").map { it.product }.toSet())
            .isEqualTo(setOf(bread, eggs, cheese))
    }

    private fun recommendationsFor(orderId: OrderId): Set<ProductWithProbability> =
        outputTopic.readKeyValuesToMap()[orderId]?.productsWithProbabilities.orEmpty().toSet()

    private fun pipeOrder(orderId: OrderId, vararg products: Product) {
        val order = faker.order().copy(
            orderId = orderId,
            orderLines = products.map { OrderLine(it.productId, it.price, 1) },
        )
        orderInputTopic.pipeInput(order.orderId, order)
    }
}
