package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.OrderId
import kotlinx.serialization.Serializable

@Serializable
data class RelatedProductWithOrder(val relatedProduct: RelatedProduct?, val orderId: OrderId, val expectedSize: Int)
