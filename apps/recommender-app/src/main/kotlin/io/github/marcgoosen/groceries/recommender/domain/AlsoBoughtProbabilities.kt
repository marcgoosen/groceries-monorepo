package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.ProductId
import kotlinx.serialization.Serializable

@Serializable
data class AlsoBoughtProbabilities(val probabilityByProductId: Map<ProductId, Double> = emptyMap())
