package io.github.marcgoosen.groceries.shared.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.datetime.Instant
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * Order is what the pipeline consumes, so this encoding is the contract with whoever produces orders.
 */
class OrderTest {
    private val json = Json { prettyPrint = true }

    private val order = Order(
        orderId = "order-1",
        orderLines = listOf(OrderLine("p1", 2.0, 1), OrderLine("p2", 1.5, 3)),
        timestamp = Instant.parse("2026-01-15T09:30:00.123Z"),
    )

    @Test
    fun `It should serialize an order to JSON and read it back`() {
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
        assertThat(json.decodeFromString<Order>(serialized)).isEqualTo(order)
    }

    @Test
    fun `It should serialize an order to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(Order.serializer(), order)

        assertThat(
            serialized.toHex(),
        ).isEqualTo("0e6f726465722d3104047031000000000000004002047032000000000000f83f0600f6a8ec8ff866")
        assertThat(Avro.decodeFromByteArray(Order.serializer(), serialized)).isEqualTo(order)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
