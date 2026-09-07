package io.github.marcgoosen.groceries.recommender.kafkastreams

import io.github.marcgoosen.groceries.recommender.AlsoBoughtTable
import io.github.marcgoosen.groceries.recommender.OrderByProductIdStream
import io.github.marcgoosen.groceries.recommender.OrderStream
import io.github.marcgoosen.groceries.recommender.ProbabilityWithOrderStream
import io.github.marcgoosen.groceries.recommender.ProductTable
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Materialized

class TopologyBuilder(val streamsBuilder: StreamsBuilder, private val topicNameBuilder: TopicNameBuilder) {
    val Topic.topicName get() = topicNameBuilder.build(this)

    val orderStream: OrderStream by lazy {
        streamsBuilder
            .stream(
                Topic.ORDER.topicName,
            )
    }
    val productTable: ProductTable by lazy {
        streamsBuilder
            .table(
                Topic.PRODUCT.topicName,
                Materialized.`as`(PRODUCT),
            )
    }

    val alsoBoughtTable: AlsoBoughtTable by lazy {
        orderStream
            .toProductPairs()
            .countAlsoBought()
    }

    fun OrderByProductIdStream.joinWithAlsoBoughtCounts() = this
        .joinWithAlsoBoughtCounts(
            alsoBoughtTable,
        )

    fun ProbabilityWithOrderStream.joinWithProduct() = this
        .joinWithProduct(
            productTable,
        )

    fun build(block: TopologyBuilder.() -> Unit): Topology = streamsBuilder
        .apply { this@TopologyBuilder.block() }.build()

    fun build(): Topology = build {
        orderStream
            .explodeOrderByProductId()
            .joinWithAlsoBoughtCounts()
            .rekeyByOrderId()
            .collectPerOrder()
            .onlyComplete()
            .sumAlsoBought()
            .toAlsoBoughtProbabilities()
            .selectTopN(3)
            .explodeByProductId()
            .joinWithProduct()
            .collectPerOrder()
            .onlyComplete()
            .removeEmpty()
            .to(Topic.RELATED_PRODUCTS.topicName)
    }
}
