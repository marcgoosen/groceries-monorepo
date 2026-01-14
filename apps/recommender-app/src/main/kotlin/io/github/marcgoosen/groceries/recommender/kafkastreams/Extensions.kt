package io.github.marcgoosen.groceries.recommender.kafkastreams

import io.github.marcgoosen.groceries.recommender.CoDistributionStream
import io.github.marcgoosen.groceries.recommender.CoOccurrenceStream
import io.github.marcgoosen.groceries.recommender.CoOccurrenceTable
import io.github.marcgoosen.groceries.recommender.CoOccurrenceWithContextByProductIdStream
import io.github.marcgoosen.groceries.recommender.CoOccurrenceWithContextStream
import io.github.marcgoosen.groceries.recommender.CoOccurrencesWithContextStream
import io.github.marcgoosen.groceries.recommender.OrderByProductIdStream
import io.github.marcgoosen.groceries.recommender.OrderStream
import io.github.marcgoosen.groceries.recommender.ProbabilityContextStream
import io.github.marcgoosen.groceries.recommender.ProductIdStream
import io.github.marcgoosen.groceries.recommender.ProductTable
import io.github.marcgoosen.groceries.recommender.ProductWithProbabilityContextStream
import io.github.marcgoosen.groceries.recommender.ProductsWithProbabilityContextStream
import io.github.marcgoosen.groceries.recommender.ProductsWithProbabilityStream
import io.github.marcgoosen.groceries.recommender.allOrderedPairs
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrencesWithContext
import io.github.marcgoosen.groceries.recommender.domain.ProbabilityContext
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbabilityContext
import io.github.marcgoosen.groceries.recommender.domain.isComplete
import io.github.marcgoosen.groceries.recommender.domain.orderId
import io.github.marcgoosen.groceries.recommender.domain.plus
import io.github.marcgoosen.groceries.recommender.domain.productIds
import io.github.marcgoosen.groceries.recommender.domain.rollup
import io.github.marcgoosen.groceries.recommender.domain.toCoDistribution
import io.github.marcgoosen.groceries.recommender.domain.toProductsWithProbability
import io.github.marcgoosen.groceries.recommender.domain.topN
import io.github.marcgoosen.groceries.recommender.domain.withContext
import io.github.marcgoosen.groceries.recommender.domain.withProduct
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named

internal const val CO_OCCURRENCE = "co-occurrence"
internal const val CO_OCCURRENCE_WITH_CONTEXT_BY_PRODUCT_ID = "co-occurrence-with-context-by-product-id"
internal const val CO_OCCURRENCE_WITH_CONTEXT = "co-occurrence-with-context"
internal const val CO_OCCURRENCES_WITH_CONTEXT = "co-occurrences-with-context"
internal const val PRODUCTS_WITH_PROBABILITY_CONTEXT = "products-with-probability-context"
internal const val ORDER_BY_PRODUCT_ID = "order-by-product-id"
internal const val PRODUCT_ID = "product-id"
internal const val PROBABILITY_CONTEXT = "probability-context"
internal const val PRODUCT_WITH_PROBABILITY_CONTEXT = "product-with-probability-context"
internal const val PRODUCT = "product"

fun OrderStream.toCoOccurrencePairs(): ProductIdStream = this
    .flatMap(
        { _, order ->
            order.productIds.toList().allOrderedPairs()
                .map {
                    KeyValue(
                        it.first,
                        it.second,
                    )
                }
        },
        Named.`as`(PRODUCT_ID),
    )

fun ProductIdStream.countCoOccurrences(storeName: String = CO_OCCURRENCE): CoOccurrenceTable = this
    .groupByKey()
    .aggregate(
        { CoOccurrence() },
        { _, productId, coOccurrence ->
            coOccurrence + productId
        },
        Named.`as`(storeName),
        Materialized.`as`(storeName),
    )

@JvmName("OrderStreamExplodeByProductIds")
fun OrderStream.explodeByProductId(): OrderByProductIdStream = this
    .flatMap(
        { _, order ->
            order.productIds.map {
                KeyValue(
                    it,
                    order,
                )
            }
        },
        Named.`as`(ORDER_BY_PRODUCT_ID),
    )

fun OrderByProductIdStream.joinWithCoOccurrences(
    productCoOccurrenceTable: KTable<ProductId, CoOccurrence>,
    storeName: String = CO_OCCURRENCE_WITH_CONTEXT_BY_PRODUCT_ID,
): CoOccurrenceWithContextByProductIdStream = this
    .leftJoin(
        productCoOccurrenceTable,
        { _, order, coOccurrence: CoOccurrence? ->
            coOccurrence.withContext(order)
        },
        Joined.`as`(storeName),
    )

fun CoOccurrenceWithContextByProductIdStream.rekeyByOrderId(
    storeName: String = CO_OCCURRENCE_WITH_CONTEXT,
): CoOccurrenceWithContextStream = this
    .map(
        { _, context ->
            KeyValue(
                context.orderId,
                context,
            )
        },
        Named.`as`(storeName),
    )

@JvmName("CoOccurrenceWithContextStreamCollectPerOrder")
fun CoOccurrenceWithContextStream.collectPerOrder(
    storeName: String = CO_OCCURRENCES_WITH_CONTEXT,
): CoOccurrencesWithContextStream = this
    .groupByKey()
    .aggregate(
        { CoOccurrencesWithContext() },
        { _, coOccurrenceWithContext, coOccurrencesWithContext ->
            coOccurrencesWithContext + coOccurrenceWithContext
        },
        Named.`as`(storeName),
        Materialized.`as`(storeName),
    )
    .toStream()

@JvmName("CoOccurrencesWithContextStreamOnlyComplete")
fun CoOccurrencesWithContextStream.onlyComplete(): CoOccurrencesWithContextStream =
    filter { _, coOccurrencesWithContext -> coOccurrencesWithContext.isComplete() }

fun CoOccurrencesWithContextStream.rollupToCoOccurrence(): CoOccurrenceStream =
    mapValues { coOccurrencesWithContext -> coOccurrencesWithContext.rollup() }

fun CoOccurrenceStream.toCoDistribution(): CoDistributionStream =
    mapValues { coOccurrence -> coOccurrence.toCoDistribution() }

fun CoDistributionStream.selectTopN(n: Int): CoDistributionStream =
    mapValues { coDistribution -> coDistribution.topN(n) }

@JvmName("CoDistributionStreamExplodeByProductIds")
fun CoDistributionStream.explodeByProductId(): ProbabilityContextStream = this
    .flatMap(
        { orderId, coDistribution ->
            coDistribution.probabilityByProductId.map { (productId, probability) ->
                KeyValue(
                    productId,
                    ProbabilityContext(
                        probability,
                        orderId,
                        coDistribution.probabilityByProductId.size,
                    ),
                )
            }
        },
        Named.`as`(PROBABILITY_CONTEXT),
    )

fun ProbabilityContextStream.joinWithProduct(productTable: ProductTable): ProductWithProbabilityContextStream = this
    .leftJoin(
        productTable,
        { context, product: Product? -> context.withProduct(product) },
        Joined.`as`(PRODUCT_WITH_PROBABILITY_CONTEXT),
    )

@JvmName("ProductWithProbabilityContextStreamCollectPerOrder")
fun ProductWithProbabilityContextStream.collectPerOrder(
    storeName: String = PRODUCTS_WITH_PROBABILITY_CONTEXT,
): ProductsWithProbabilityContextStream = this
    .groupBy { _, context -> context.orderId }
    .aggregate(
        { ProductsWithProbabilityContext() },
        { _, productWithProbabilityContext, productsWithProbabilityContext ->
            productsWithProbabilityContext + productWithProbabilityContext
        },
        Named.`as`(storeName),
        Materialized.`as`(storeName),
    )
    .toStream()

@JvmName("ProductsWithProbabilityContextStreamOnlyComplete")
fun ProductsWithProbabilityContextStream.onlyComplete(): ProductsWithProbabilityContextStream =
    filter { _, context -> context.isComplete() }

fun ProductsWithProbabilityContextStream.removeEmpty(): ProductsWithProbabilityStream =
    mapValues { _, context -> context.toProductsWithProbability() }
