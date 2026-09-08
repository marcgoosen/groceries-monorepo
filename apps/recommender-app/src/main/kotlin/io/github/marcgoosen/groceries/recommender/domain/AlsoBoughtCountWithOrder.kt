package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.Order
import kotlinx.serialization.Serializable

@Serializable
data class AlsoBoughtCountWithOrder(val alsoBought: AlsoBoughtCount, val order: Order)
