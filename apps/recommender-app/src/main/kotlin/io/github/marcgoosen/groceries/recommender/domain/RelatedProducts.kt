package io.github.marcgoosen.groceries.recommender.domain

import kotlinx.serialization.Serializable

@Serializable
data class RelatedProducts(val relatedProducts: List<RelatedProduct>)
