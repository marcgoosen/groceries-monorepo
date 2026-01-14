package io.github.marcgoosen.groceries.shared.domain

import io.github.marcgoosen.groceries.shared.kafka.InstantSerializer
import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable

@Serializable
data class Order(
    val orderId: OrderId,
    val orderLines: List<OrderLine>,
    @Serializable(with = InstantSerializer::class) val timestamp: Instant,
)
