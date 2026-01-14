package io.github.marcgoosen.groceries.shared.domain

import kotlinx.serialization.Serializable

@Serializable
data class OrderLine(val productId: ProductId, val price: Double, val quantity: Int)
