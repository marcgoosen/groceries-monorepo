package io.github.marcgoosen.groceries.recommender

import io.github.marcgoosen.groceries.recommender.kafkastreams.Topic
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopicNameBuilder
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.serpro69.kfaker.Faker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Clock
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

private val logger = KotlinLogging.logger {}

private const val CATALOGUE_SIZE = 99
private const val MAX_ORDER_LINES = 5
private const val MAX_QUANTITY = 3
private val ORDER_INTERVAL = 50.milliseconds

class Simulator(
    private val producers: Producers,
    private val topicNameBuilder: TopicNameBuilder,
    private val orderInterval: Duration = ORDER_INTERVAL,
) : AutoCloseable {
    data class Producers(val product: Producer<ProductId, Product>, val order: Producer<OrderId, Order>)

    private val faker = Faker()
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val Topic.topicName get() = topicNameBuilder.build(this)

    private val products =
        (1..CATALOGUE_SIZE).map {
            Product(
                productId = "product-${it.toString().padStart(3, '0')}",
                name = faker.food.ingredients(),
                price = faker.random.nextInt(1, 20).toDouble() + 0.99,
            )
        }

    fun start() {
        logger.info { "Starting simulator..." }
        produceProducts()
        scope.launch {
            while (isActive) {
                delay(orderInterval)
                runCatching { produceOrder() }
                    .onFailure { logger.error(it) { "Failed to produce order" } }
            }
        }
    }

    override fun close() {
        runBlocking { scope.coroutineContext.job.cancelAndJoin() }
        producers.product.close()
        producers.order.close()
    }

    private fun produceProducts() {
        val productTopic = Topic.PRODUCT.topicName
        products.forEach { product ->
            producers.product.send(ProducerRecord(productTopic, product.productId, product))
        }
        producers.product.flush()
        logger.info { "Produced ${products.size} products to $productTopic" }
    }

    private fun produceOrder() {
        val order = generateRandomOrder()
        producers.order.send(ProducerRecord(Topic.ORDER.topicName, order.orderId, order)) { metadata, exception ->
            when (exception) {
                null -> logger.debug { "Produced order ${order.orderId} to ${metadata.topic()}" }
                else -> logger.error(exception) { "Error producing order ${order.orderId}" }
            }
        }
    }

    private fun generateRandomOrder(): Order = Order(
        orderId = UUID.randomUUID().toString(),
        timestamp = Clock.System.now(),
        orderLines = generateRandomOrderLines(),
    )

    private fun generateRandomOrderLines(): List<OrderLine> = (1..faker.random.nextInt(1, MAX_ORDER_LINES))
        .map { products.random() }
        .distinctBy { it.productId }
        .map { product ->
            OrderLine(
                productId = product.productId,
                price = product.price,
                quantity = faker.random.nextInt(1, MAX_QUANTITY),
            )
        }
}
