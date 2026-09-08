package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.Order
import kotlinx.serialization.Serializable

@Serializable
data class AlsoBoughtSoFar(val alsoBought: List<AlsoBoughtCount> = emptyList(), val order: Order? = null)
