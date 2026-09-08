package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.OrderId
import kotlinx.serialization.Serializable

@Serializable
data class ProbabilityWithOrder(val probability: Double, val orderId: OrderId, val expectedSize: Int)
