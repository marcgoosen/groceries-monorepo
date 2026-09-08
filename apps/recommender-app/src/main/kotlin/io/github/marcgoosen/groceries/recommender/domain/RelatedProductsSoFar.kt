package io.github.marcgoosen.groceries.recommender.domain

import kotlinx.serialization.Serializable

@Serializable
data class RelatedProductsSoFar(val relatedProducts: List<RelatedProduct?> = emptyList(), val expectedSize: Int = 0)
