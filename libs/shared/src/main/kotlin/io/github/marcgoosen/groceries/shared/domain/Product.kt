package io.github.marcgoosen.groceries.shared.domain

import kotlinx.serialization.Serializable

@Serializable
data class Product(val productId: ProductId, val name: String, val price: Double)
