package io.github.marcgoosen.groceries.shared.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.datetime.Instant
import org.junit.jupiter.api.Test

/**
 * Order is what the pipeline consumes, so its Avro encoding is the contract with producers. It has no JSON
 * counterpart: InstantSerializer extends avro4k's AvroSerializer and throws outside an Avro encoder.
 */
class OrderTest {
    private val order = Order(
        orderId = "order-1",
        orderLines = listOf(
            OrderLine(productId = "p1", price = 2.0, quantity = 1),
            OrderLine(productId = "p2", price = 1.5, quantity = 3),
        ),
        timestamp = Instant.parse("2026-01-15T09:30:00.123Z"),
    )

    @Test
    fun `It should round-trip through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            Order.serializer(),
            Avro.encodeToByteArray(Order.serializer(), order),
        )

        assertThat(roundTripped).isEqualTo(order)
    }

    @Test
    fun `It should round-trip an order with no lines`() {
        // Given
        val empty = order.copy(orderLines = emptyList())

        // When
        val roundTripped = Avro.decodeFromByteArray(
            Order.serializer(),
            Avro.encodeToByteArray(Order.serializer(), empty),
        )

        assertThat(roundTripped).isEqualTo(empty)
    }
}
