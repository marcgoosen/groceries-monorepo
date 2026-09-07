package io.github.marcgoosen.groceries.shared.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * A single line of an order. It carries the price paid at the time, which is not necessarily the catalogue price.
 */
class OrderLineTest {
    private val json = Json { prettyPrint = true }

    private val orderLine = OrderLine(productId = "p1", price = 2.49, quantity = 2)

    @Test
    fun `It should round-trip an order line through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<OrderLine>(json.encodeToString(orderLine))

        assertThat(roundTripped).isEqualTo(orderLine)
    }

    @Test
    fun `It should round-trip an order line through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            OrderLine.serializer(),
            Avro.encodeToByteArray(OrderLine.serializer(), orderLine),
        )

        assertThat(roundTripped).isEqualTo(orderLine)
    }

    @Test
    fun `It should round-trip a line that was given away for free`() {
        // Given
        val free = orderLine.copy(price = 0.0)

        // When
        val roundTripped = Avro.decodeFromByteArray(
            OrderLine.serializer(),
            Avro.encodeToByteArray(OrderLine.serializer(), free),
        )

        assertThat(roundTripped).isEqualTo(free)
    }
}
