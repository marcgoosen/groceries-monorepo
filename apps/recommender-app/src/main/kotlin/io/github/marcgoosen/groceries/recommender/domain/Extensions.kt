package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId

val Order.productIds get() = orderLines.map { it.productId }.toSet()

val AlsoBoughtCount.totalCount get() = countsByProduct.values.sum()

fun AlsoBoughtSoFar.isComplete() = alsoBought.size == (order?.productIds?.size ?: 0)

operator fun AlsoBoughtSoFar.plus(alsoBoughtCountWithOrder: AlsoBoughtCountWithOrder) = AlsoBoughtSoFar(
    order = alsoBoughtCountWithOrder.order,
    alsoBought = alsoBought + alsoBoughtCountWithOrder.alsoBought,
)

fun AlsoBoughtSoFar.sum() = alsoBought.sum() - (order?.productIds ?: emptySet())

fun AlsoBoughtCount?.withOrder(order: Order) = AlsoBoughtCountWithOrder(this ?: AlsoBoughtCount(), order)

val AlsoBoughtCountWithOrder.orderId get() = order.orderId

fun RelatedProductsSoFar.isComplete() = relatedProducts.size == expectedSize

fun RelatedProductsSoFar.toRelatedProducts() = RelatedProducts(relatedProducts.filterNotNull())

operator fun RelatedProductsSoFar.plus(relatedProductWithOrder: RelatedProductWithOrder) = RelatedProductsSoFar(
    expectedSize = relatedProductWithOrder.expectedSize,
    relatedProducts =
    relatedProducts + relatedProductWithOrder.relatedProduct,
)

fun Product.withProbability(probability: Double) = RelatedProduct(this, probability)

operator fun AlsoBoughtCount.plus(productId: ProductId) = copy(
    countsByProduct = countsByProduct + (productId to (countsByProduct[productId]?.plus(1) ?: 1)),
)

operator fun AlsoBoughtCount.minus(productId: ProductId) = copy(
    countsByProduct = countsByProduct - productId,
)

operator fun AlsoBoughtCount.minus(productIds: Set<ProductId>) = copy(
    countsByProduct = countsByProduct - productIds,
)

operator fun AlsoBoughtCount.plus(other: AlsoBoughtCount) = AlsoBoughtCount(
    countsByProduct = (countsByProduct.keys + other.countsByProduct.keys).associateWith { key ->
        (countsByProduct[key] ?: 0) + (other.countsByProduct[key] ?: 0)
    },
)

fun AlsoBoughtCount.toAlsoBoughtProbabilities() = totalCount
    .takeUnless { it == 0 }
    ?.let { total ->
        AlsoBoughtProbabilities(
            probabilityByProductId = countsByProduct.mapValues { (_, count) -> count.toDouble() / total },
        )
    } ?: AlsoBoughtProbabilities()

fun AlsoBoughtProbabilities.topN(n: Int): AlsoBoughtProbabilities = AlsoBoughtProbabilities(
    probabilityByProductId = probabilityByProductId.entries
        .asSequence()
        .sortedByDescending { it.value }
        .take(n)
        .associate { it.key to it.value },
)

fun List<AlsoBoughtCount>.sum() = fold(AlsoBoughtCount()) { acc, pc -> acc + pc }

fun ProbabilityWithOrder.withProduct(product: Product?) = RelatedProductWithOrder(
    relatedProduct = product?.withProbability(probability),
    orderId = orderId,
    expectedSize = expectedSize,
)
