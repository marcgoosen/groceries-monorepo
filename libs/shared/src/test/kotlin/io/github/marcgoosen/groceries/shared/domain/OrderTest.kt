package io.github.marcgoosen.groceries.shared.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.datetime.Instant
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * Order is what the pipeline consumes, so its Avro encoding is the contract with whoever produces orders.
 */
class OrderTest {
    private val json = Json { prettyPrint = true }

    private val order = Order(
        orderId = "order-1",
        orderLines = listOf(OrderLine("p1", 2.0, 1), OrderLine("p2", 1.5, 3)),
        timestamp = Instant.parse("2026-01-15T09:30:00.123Z"),
    )

    @Test
    fun `It should round-trip an order through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<Order>(json.encodeToString(order))

        assertThat(roundTripped).isEqualTo(order)
    }

    @Test
    fun `It should round-trip an order through Avro`() {
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

    @Test
    fun `It should carry the timestamp as epoch milliseconds in JSON`() {
        // Given
        // When
        val serialized = json.encodeToString(order)

        assertThat(serialized).isEqualTo(
            """
            {
                "orderId": "order-1",
                "orderLines": [
                    {
                        "productId": "p1",
                        "price": 2.0,
                        "quantity": 1
                    },
                    {
                        "productId": "p2",
                        "price": 1.5,
                        "quantity": 3
                    }
                ],
                "timestamp": 1768469400123
            }
            """.trimIndent(),
        )
    }
}
