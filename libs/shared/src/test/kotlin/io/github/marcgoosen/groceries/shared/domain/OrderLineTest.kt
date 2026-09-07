package io.github.marcgoosen.groceries.shared.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * A single line of an order, carrying the price paid at the time rather than the catalogue price.
 */
class OrderLineTest {
    private val json = Json { prettyPrint = true }

    private val orderLine = OrderLine(productId = "p1", price = 2.49, quantity = 2)

    @Test
    fun `It should serialize an order line to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(OrderLine.serializer(), orderLine)

        assertThat(serialized).isEqualTo(
            """
            {
                "productId": "p1",
                "price": 2.49,
                "quantity": 2
            }
            """.trimIndent(),
        )
        assertThat(json.decodeFromString(OrderLine.serializer(), serialized)).isEqualTo(orderLine)
    }

    @Test
    fun `It should serialize an order line to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(OrderLine.serializer(), orderLine)

        assertThat(serialized.toHex()).isEqualTo("047031ec51b81e85eb034004")
        assertThat(Avro.decodeFromByteArray(OrderLine.serializer(), serialized)).isEqualTo(orderLine)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
