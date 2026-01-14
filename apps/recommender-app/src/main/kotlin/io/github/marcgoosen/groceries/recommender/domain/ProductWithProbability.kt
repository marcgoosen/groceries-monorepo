package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.Product
import kotlinx.serialization.Serializable

@Serializable
data class ProductWithProbability(val product: Product, val probability: Double)
