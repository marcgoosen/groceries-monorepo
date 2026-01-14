package io.github.marcgoosen.groceries.recommender.kafkastreams

import io.github.marcgoosen.groceries.recommender.CoOccurrenceTable
import io.github.marcgoosen.groceries.recommender.OrderByProductIdStream
import io.github.marcgoosen.groceries.recommender.OrderStream
import io.github.marcgoosen.groceries.recommender.ProbabilityContextStream
import io.github.marcgoosen.groceries.recommender.ProductTable
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Materialized

class TopologyBuilder(
    val streamsBuilder: StreamsBuilder,
    private val avroSerdes: AvroSerdes,
    private val topicNameBuilder: TopicNameBuilder,
) {
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

    val productCoOccurrenceTable: CoOccurrenceTable by lazy {
        orderStream
            .toCoOccurrencePairs()
            .countCoOccurrences()
    }

    fun OrderByProductIdStream.joinWithCoOccurrences() = this
        .joinWithCoOccurrences(
            productCoOccurrenceTable,
        )

    fun ProbabilityContextStream.joinWithProduct() = this
        .joinWithProduct(
            productTable,
        )

    fun build(block: TopologyBuilder.() -> Unit): Topology = streamsBuilder
        .apply { this@TopologyBuilder.block() }.build()

    fun build(): Topology = build {
        orderStream
            .explodeByProductId()
            .joinWithCoOccurrences()
            .rekeyByOrderId()
            .collectPerOrder()
            .onlyComplete()
            .rollupToCoOccurrence()
            .toCoDistribution()
            .selectTopN(3)
            .explodeByProductId()
            .joinWithProduct()
            .collectPerOrder()
            .onlyComplete()
            .removeEmpty()
            .to(Topic.RELATED_PRODUCTS.topicName)
    }
}
