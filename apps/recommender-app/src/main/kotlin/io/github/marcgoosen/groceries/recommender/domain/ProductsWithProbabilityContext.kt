package io.github.marcgoosen.groceries.recommender.domain

import kotlinx.serialization.Serializable

@Serializable
data class ProductsWithProbabilityContext(
    val productsWithProbabilities: List<ProductWithProbability?> = emptyList(),
    val expectedSize: Int = 0,
)
