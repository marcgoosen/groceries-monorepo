package io.github.marcgoosen.groceries.recommender.kafkastreams

import io.github.marcgoosen.groceries.recommender.AlsoBoughtCountWithOrderByProductIdStream
import io.github.marcgoosen.groceries.recommender.AlsoBoughtCountWithOrderStream
import io.github.marcgoosen.groceries.recommender.AlsoBoughtProbabilitiesStream
import io.github.marcgoosen.groceries.recommender.AlsoBoughtSoFarStream
import io.github.marcgoosen.groceries.recommender.AlsoBoughtStream
import io.github.marcgoosen.groceries.recommender.AlsoBoughtTable
import io.github.marcgoosen.groceries.recommender.OrderByProductIdStream
import io.github.marcgoosen.groceries.recommender.OrderStream
import io.github.marcgoosen.groceries.recommender.ProbabilityWithOrderStream
import io.github.marcgoosen.groceries.recommender.ProductIdStream
import io.github.marcgoosen.groceries.recommender.ProductTable
import io.github.marcgoosen.groceries.recommender.RelatedProductWithOrderStream
import io.github.marcgoosen.groceries.recommender.RelatedProductsSoFarStream
import io.github.marcgoosen.groceries.recommender.RelatedProductsStream
import io.github.marcgoosen.groceries.recommender.allOrderedPairs
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCount
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtSoFar
import io.github.marcgoosen.groceries.recommender.domain.ProbabilityWithOrder
import io.github.marcgoosen.groceries.recommender.domain.RelatedProductsSoFar
import io.github.marcgoosen.groceries.recommender.domain.isComplete
import io.github.marcgoosen.groceries.recommender.domain.orderId
import io.github.marcgoosen.groceries.recommender.domain.plus
import io.github.marcgoosen.groceries.recommender.domain.productIds
import io.github.marcgoosen.groceries.recommender.domain.sum
import io.github.marcgoosen.groceries.recommender.domain.toAlsoBoughtProbabilities
import io.github.marcgoosen.groceries.recommender.domain.toRelatedProducts
import io.github.marcgoosen.groceries.recommender.domain.topN
import io.github.marcgoosen.groceries.recommender.domain.withOrder
import io.github.marcgoosen.groceries.recommender.domain.withProduct
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named

internal const val ALSO_BOUGHT = "also-bought"
internal const val ALSO_BOUGHT_COUNT_WITH_ORDER_BY_PRODUCT_ID =
    "also-bought-count-with-order-by-product-id"
internal const val ALSO_BOUGHT_COUNT_WITH_ORDER = "also-bought-count-with-order"
internal const val ALSO_BOUGHT_SO_FAR = "also-bought-so-far"
internal const val RELATED_PRODUCTS_SO_FAR = "related-products-so-far"
internal const val ORDER_BY_PRODUCT_ID = "order-by-product-id"
internal const val PRODUCT_ID = "product-id"
internal const val PROBABILITY_WITH_ORDER = "probability-with-order"
internal const val RELATED_PRODUCT_WITH_ORDER = "related-product-with-order"
internal const val PRODUCT = "product"

fun OrderStream.toProductPairs(): ProductIdStream = this
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

fun ProductIdStream.countAlsoBought(storeName: String = ALSO_BOUGHT): AlsoBoughtTable = this
    .groupByKey()
    .aggregate(
        { AlsoBoughtCount() },
        { _, productId, alsoBought ->
            alsoBought + productId
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

fun OrderByProductIdStream.joinWithAlsoBought(
    alsoBoughtTable: KTable<ProductId, AlsoBoughtCount>,
    storeName: String = ALSO_BOUGHT_COUNT_WITH_ORDER_BY_PRODUCT_ID,
): AlsoBoughtCountWithOrderByProductIdStream = this
    .leftJoin(
        alsoBoughtTable,
        { _, order, alsoBought: AlsoBoughtCount? ->
            alsoBought.withOrder(order)
        },
        Joined.`as`(storeName),
    )

fun AlsoBoughtCountWithOrderByProductIdStream.rekeyByOrderId(
    storeName: String = ALSO_BOUGHT_COUNT_WITH_ORDER,
): AlsoBoughtCountWithOrderStream = this
    .map(
        { _, context ->
            KeyValue(
                context.orderId,
                context,
            )
        },
        Named.`as`(storeName),
    )

@JvmName("AlsoBoughtCountWithOrderStreamCollectPerOrder")
fun AlsoBoughtCountWithOrderStream.collectPerOrder(storeName: String = ALSO_BOUGHT_SO_FAR): AlsoBoughtSoFarStream = this
    .groupByKey()
    .aggregate(
        { AlsoBoughtSoFar() },
        { _, alsoBoughtCountWithOrder, alsoBoughtSoFar ->
            alsoBoughtSoFar + alsoBoughtCountWithOrder
        },
        Named.`as`(storeName),
        Materialized.`as`(storeName),
    )
    .toStream()

@JvmName("AlsoBoughtSoFarStreamOnlyComplete")
fun AlsoBoughtSoFarStream.onlyComplete(): AlsoBoughtSoFarStream =
    filter { _, alsoBoughtSoFar -> alsoBoughtSoFar.isComplete() }

fun AlsoBoughtSoFarStream.sumAlsoBought(): AlsoBoughtStream = mapValues { alsoBoughtSoFar -> alsoBoughtSoFar.sum() }

fun AlsoBoughtStream.toAlsoBoughtProbabilities(): AlsoBoughtProbabilitiesStream =
    mapValues { alsoBought -> alsoBought.toAlsoBoughtProbabilities() }

fun AlsoBoughtProbabilitiesStream.selectTopN(n: Int): AlsoBoughtProbabilitiesStream =
    mapValues { alsoBoughtProbabilities -> alsoBoughtProbabilities.topN(n) }

@JvmName("AlsoBoughtProbabilitiesStreamExplodeByProductIds")
fun AlsoBoughtProbabilitiesStream.explodeByProductId(): ProbabilityWithOrderStream = this
    .flatMap(
        { orderId, alsoBoughtProbabilities ->
            alsoBoughtProbabilities.probabilityByProductId.map { (productId, probability) ->
                KeyValue(
                    productId,
                    ProbabilityWithOrder(
                        probability,
                        orderId,
                        alsoBoughtProbabilities.probabilityByProductId.size,
                    ),
                )
            }
        },
        Named.`as`(PROBABILITY_WITH_ORDER),
    )

fun ProbabilityWithOrderStream.joinWithProduct(productTable: ProductTable): RelatedProductWithOrderStream = this
    .leftJoin(
        productTable,
        { context, product: Product? -> context.withProduct(product) },
        Joined.`as`(RELATED_PRODUCT_WITH_ORDER),
    )

@JvmName("RelatedProductWithOrderStreamCollectPerOrder")
fun RelatedProductWithOrderStream.collectPerOrder(
    storeName: String = RELATED_PRODUCTS_SO_FAR,
): RelatedProductsSoFarStream = this
    .groupBy { _, context -> context.orderId }
    .aggregate(
        { RelatedProductsSoFar() },
        { _, relatedProductWithOrder, relatedProductsSoFar ->
            relatedProductsSoFar + relatedProductWithOrder
        },
        Named.`as`(storeName),
        Materialized.`as`(storeName),
    )
    .toStream()

@JvmName("RelatedProductsSoFarStreamOnlyComplete")
fun RelatedProductsSoFarStream.onlyComplete(): RelatedProductsSoFarStream =
    filter { _, context -> context.isComplete() }

fun RelatedProductsSoFarStream.removeEmpty(): RelatedProductsStream =
    mapValues { _, context -> context.toRelatedProducts() }
