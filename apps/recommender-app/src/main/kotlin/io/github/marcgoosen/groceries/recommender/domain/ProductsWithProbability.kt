package io.github.marcgoosen.groceries.recommender.domain

import kotlinx.serialization.Serializable

@Serializable
data class ProductsWithProbability(val productsWithProbabilities: List<ProductWithProbability>)
