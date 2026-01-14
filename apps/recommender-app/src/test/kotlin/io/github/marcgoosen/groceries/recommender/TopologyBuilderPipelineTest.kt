package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbability
import io.github.marcgoosen.groceries.recommender.kafkastreams.Topic
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import io.kotest.matchers.shouldBe
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@OptIn(ExperimentalAvro4kApi::class)
class TopologyBuilderPipelineTest : BaseTopologyBuilderTest() {
    private lateinit var orderInputTopic: TestInputTopic<OrderId, Order>
    private lateinit var productInputTopic: TestInputTopic<ProductId, Product>
    private lateinit var outputTopic: TestOutputTopic<OrderId, ProductsWithProbability>

    @BeforeEach
    fun setup() {
        super.setup {
            build() // This executes the hardcoded build() in TopologyBuilder
        }

        orderInputTopic =
            topologyTestDriver.createInputTopic(
                Topic.ORDER.topicName,
                avroSerdes.string.serializer(),
                avroSerdes.create<Order>().serializer(),
            )

        productInputTopic =
            topologyTestDriver.createInputTopic(
                Topic.PRODUCT.topicName,
                avroSerdes.string.serializer(),
                avroSerdes.create<Product>().serializer(),
            )

        // Output topic is hardcoded to "henk" in TopologyBuilder.kt
        outputTopic =
            topologyTestDriver.createOutputTopic(
                Topic.RELATED_PRODUCTS.topicName,
                avroSerdes.string.deserializer(),
                avroSerdes.create<ProductsWithProbability>().deserializer(),
            )
    }

    @Test
    fun `It should recommend products based on co-occurrence in previous orders`() {
        // 1. Setup products
        val p1 = Product("p1", "Milk", 2.0)
        val p2 = Product("p2", "Bread", 1.5)
        val p3 = Product("p3", "Eggs", 3.0)
        val p4 = Product("p4", "Cheese", 4.0)

        productInputTopic.pipeInput(p1.productId, p1)
        productInputTopic.pipeInput(p2.productId, p2)
        productInputTopic.pipeInput(p3.productId, p3)
        productInputTopic.pipeInput(p4.productId, p4)

        // 2. Prime co-occurrences with an order containing p1, p2, p3
        val trainingOrder =
            faker.order()
                .copy(
                    orderId = "training-1",
                    orderLines =
                    listOf(
                        OrderLine(p1.productId, p1.price, 1),
                        OrderLine(p2.productId, p2.price, 1),
                        OrderLine(p3.productId, p3.price, 1),
                    ),
                )
        orderInputTopic.pipeInput(trainingOrder.orderId, trainingOrder)

        // 3. Send a new order with just p1.
        // We expect it to recommend p2 and p3 because they co-occurred with p1 in training-1.
        val targetOrder =
            faker.order()
                .copy(
                    orderId = "target-1",
                    orderLines = listOf(OrderLine(p1.productId, p1.price, 1)),
                )
        orderInputTopic.pipeInput(targetOrder.orderId, targetOrder)

        val results = outputTopic.readKeyValuesToMap()

        // We expect an entry for target-1
        results.containsKey(targetOrder.orderId) shouldBe true

        val recommendations = results[targetOrder.orderId]!!
        val recommendedProductIds =
            recommendations.productsWithProbabilities.map { it.product.productId }.toSet()

        // Should recommend p2 and p3 (they co-occurred with p1)
        // Should NOT recommend p1 (it's already in the order)
        // Should NOT recommend p4 (no co-occurrence)
        recommendedProductIds shouldBe setOf(p2.productId, p3.productId)

        // Probabilities should be 0.5 (1 co-occurrence out of 2 total co-occurrences for p1)
        recommendations.productsWithProbabilities.forEach { it.probability shouldBe 0.5 }
    }

    @Test
    fun `It should prioritize top N recommendations`() {
        // 1. Setup products
        val p1 = Product("p1", "Base", 1.0)
        val p2 = Product("p2", "Frequent", 1.0)
        val p3 = Product("p3", "Frequent", 1.0)
        val p4 = Product("p4", "Frequent", 1.0)
        val p5 = Product("p5", "Rare", 1.0)

        listOf(p1, p2, p3, p4, p5).forEach { productInputTopic.pipeInput(it.productId, it) }

        // 2. Create multiple orders to skew probabilities
        // p1 with p2, p3, p4 (3 times)
        repeat(3) { i ->
            orderInputTopic.pipeInput(
                "skew-1-$i",
                faker.order()
                    .copy(
                        orderLines =
                        listOf(
                            OrderLine(p1.productId, 1.0, 1),
                            OrderLine(p2.productId, 1.0, 1),
                            OrderLine(p3.productId, 1.0, 1),
                            OrderLine(p4.productId, 1.0, 1),
                        ),
                    ),
            )
        }

        // p1 with p5 (only once)
        orderInputTopic.pipeInput(
            "skew-2",
            faker.order()
                .copy(
                    orderLines =
                    listOf(
                        OrderLine(p1.productId, 1.0, 1),
                        OrderLine(p5.productId, 1.0, 1),
                    ),
                ),
        )

        // 3. Send order with p1.
        // Top N is 3 in TopologyBuilder.
        // Candidates for p1: p2 (3), p3 (3), p4 (3), p5 (1)
        // Top 3 should be p2, p3, p4. p5 should be excluded.
        val targetOrder =
            faker.order()
                .copy(
                    orderId = "target-skew",
                    orderLines = listOf(OrderLine(p1.productId, 1.0, 1)),
                )
        orderInputTopic.pipeInput(targetOrder.orderId, targetOrder)

        val results = outputTopic.readKeyValuesToMap()
        val recommendations = results[targetOrder.orderId]!!
        val recommendedProductIds =
            recommendations.productsWithProbabilities.map { it.product.productId }.toSet()

        recommendedProductIds shouldBe setOf(p2.productId, p3.productId, p4.productId)
        recommendedProductIds.contains(p5.productId) shouldBe false
    }
}
