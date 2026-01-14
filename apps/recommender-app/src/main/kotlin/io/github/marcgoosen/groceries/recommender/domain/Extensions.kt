package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId

val Order.productIds get() = orderLines.map { it.productId }.toSet()

val CoOccurrence.totalCount get() = countsByProduct.values.sum()

fun CoOccurrencesWithContext.isComplete() = coOccurrences.size == (order?.productIds?.size ?: 0)

operator fun CoOccurrencesWithContext.plus(coOccurrenceWithContext: CoOccurrenceWithContext) = CoOccurrencesWithContext(
    order = coOccurrenceWithContext.order,
    coOccurrences = coOccurrences + coOccurrenceWithContext.coOccurrence,
)

fun CoOccurrencesWithContext.rollup() = coOccurrences.rollup() - (order?.productIds ?: emptySet())

fun CoOccurrence?.withContext(order: Order) = CoOccurrenceWithContext(this ?: CoOccurrence(), order)

val CoOccurrenceWithContext.orderId get() = order.orderId

fun ProductsWithProbabilityContext.isComplete() = productsWithProbabilities.size == expectedSize

fun ProductsWithProbabilityContext.toProductsWithProbability() =
    ProductsWithProbability(productsWithProbabilities.filterNotNull())

operator fun ProductsWithProbabilityContext.plus(productWithProbabilityContext: ProductWithProbabilityContext) =
    ProductsWithProbabilityContext(
        expectedSize = productWithProbabilityContext.expectedSize,
        productsWithProbabilities =
        productsWithProbabilities + productWithProbabilityContext.productWithProbability,
    )

fun Product.withProbability(probability: Double) = ProductWithProbability(this, probability)

operator fun CoOccurrence.plus(productId: ProductId) = copy(
    countsByProduct = countsByProduct + (productId to (countsByProduct[productId]?.plus(1) ?: 1)),
)

operator fun CoOccurrence.minus(productId: ProductId) = copy(
    countsByProduct = countsByProduct - productId,
)

operator fun CoOccurrence.minus(productIds: Set<ProductId>) = copy(
    countsByProduct = countsByProduct - productIds,
)

operator fun CoOccurrence.plus(other: CoOccurrence) = CoOccurrence(
    countsByProduct = (countsByProduct.keys + other.countsByProduct.keys).associateWith { key ->
        (countsByProduct[key] ?: 0) + (other.countsByProduct[key] ?: 0)
    },
)

fun CoOccurrence.toCoDistribution() = totalCount
    .takeUnless { it == 0 }
    ?.let { total ->
        CoDistribution(
            probabilityByProductId = countsByProduct.mapValues { (_, count) -> count.toDouble() / total },
        )
    } ?: CoDistribution()

fun CoDistribution.topN(n: Int): CoDistribution = CoDistribution(
    probabilityByProductId = probabilityByProductId.entries
        .asSequence()
        .sortedByDescending { it.value }
        .take(n)
        .associate { it.key to it.value },
)

fun List<CoOccurrence>.rollup() = fold(CoOccurrence()) { acc, pc -> acc + pc }

fun ProbabilityContext.withProduct(product: Product?) = ProductWithProbabilityContext(
    productWithProbability = product?.withProbability(probability),
    orderId = orderId,
    expectedSize = expectedSize,
)
