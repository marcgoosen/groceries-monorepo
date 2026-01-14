package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.OrderId
import kotlinx.serialization.Serializable

@Serializable
data class ProductWithProbabilityContext(
    val productWithProbability: ProductWithProbability?,
    val orderId: OrderId,
    val expectedSize: Int,
)
