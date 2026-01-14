package io.github.marcgoosen.groceries.recommender

import io.github.marcgoosen.groceries.recommender.kafkastreams.AvroSerdes
import io.github.marcgoosen.groceries.recommender.kafkastreams.Topic
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopicNameBuilder
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.serpro69.kfaker.Faker
import kotlinx.datetime.Clock
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.UUID
import kotlin.concurrent.timer

private val logger = KotlinLogging.logger {}

class Simulator(
    private val kafkaConfig: Map<String, String>,
    private val avroSerdes: AvroSerdes,
    private val topicNameBuilder: TopicNameBuilder,
) {
    private val faker = Faker()
    val Topic.topicName get() = topicNameBuilder.build(this)

    private val productProducer =
        KafkaProducer<String, Product>(
            kafkaConfig.toProperties(),
            avroSerdes.string.serializer(),
            avroSerdes.create<Product>().serializer(),
        )

    private val orderProducer =
        KafkaProducer<String, Order>(
            kafkaConfig.toProperties(),
            avroSerdes.string.serializer(),
            avroSerdes.create<Order>().serializer(),
        )

    private val products =
        (1..99).map {
            Product(
                productId = "product-${it.toString().padStart(3, '0')}",
                name = faker.food.ingredients(),
                price = faker.random.nextInt(1, 20).toDouble() + 0.99,
            )
        }

    fun start() {
        logger.info { "Starting simulator..." }

        // Produce products first
        val productTopic = Topic.PRODUCT.topicName
        products.forEach { product ->
            productProducer.send(ProducerRecord(productTopic, product.productId, product))
        }
        productProducer.flush()
        logger.info { "Produced ${products.size} products to $productTopic" }

        // Periodically produce orders
        val orderTopic = Topic.ORDER.topicName
        timer("order-simulator", period = 50) {
            try {
                val order = generateRandomOrder()
                orderProducer.send(ProducerRecord(orderTopic, order.orderId, order)) {
                        metadata,
                        exception,
                    ->
                    if (exception != null) {
                        logger.error(exception) { "Error producing order" }
                    } else {
                        logger.debug { "Produced order ${order.orderId} to ${metadata.topic()}" }
                    }
                }
            } catch (e: Exception) {
                logger.error(e) { "Error in order simulator loop" }
            }
        }
    }

    private fun generateRandomOrder(): Order {
        val numLines = faker.random.nextInt(1, 5)
        val selectedProducts = (1..numLines).map { products.random() }.distinctBy { it.productId }
        val orderLines =
            selectedProducts.map { product ->
                OrderLine(
                    productId = product.productId,
                    price = product.price,
                    quantity = faker.random.nextInt(1, 3),
                )
            }

        return Order(
            orderId = UUID.randomUUID().toString(),
            timestamp = Clock.System.now(),
            orderLines = orderLines,
        )
    }

    fun close() {
        productProducer.close()
        orderProducer.close()
    }
}
