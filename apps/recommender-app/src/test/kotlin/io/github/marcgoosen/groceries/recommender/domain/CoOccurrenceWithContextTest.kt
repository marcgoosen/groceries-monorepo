package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import kotlinx.datetime.Instant
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * One product's co-occurrences carried together with the order that triggered the lookup.
 */
class CoOccurrenceWithContextTest {
    private val json = Json { prettyPrint = true }

    private val order = Order(
        orderId = "order-1",
        orderLines = listOf(OrderLine("p1", 2.0, 1), OrderLine("p2", 1.5, 3)),
        timestamp = Instant.parse("2026-01-15T09:30:00.123Z"),
    )

    private val context = CoOccurrenceWithContext(CoOccurrence(mapOf("p3" to 4)), order)

    @Test
    fun `It should round-trip a populated context through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<CoOccurrenceWithContext>(json.encodeToString(context))

        assertThat(roundTripped).isEqualTo(context)
    }

    @Test
    fun `It should round-trip a populated context through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            CoOccurrenceWithContext.serializer(),
            Avro.encodeToByteArray(CoOccurrenceWithContext.serializer(), context),
        )

        assertThat(roundTripped).isEqualTo(context)
    }

    @Test
    fun `It should round-trip a context whose product had no co-occurrences yet`() {
        // Given
        val withoutCounts = context.copy(coOccurrence = CoOccurrence())

        // When
        val roundTripped = Avro.decodeFromByteArray(
            CoOccurrenceWithContext.serializer(),
            Avro.encodeToByteArray(CoOccurrenceWithContext.serializer(), withoutCounts),
        )

        assertThat(roundTripped).isEqualTo(withoutCounts)
    }

    @Test
    fun `It should round-trip a context for an order with no lines`() {
        // Given
        val emptyOrder = context.copy(order = order.copy(orderLines = emptyList()))

        // When
        val roundTripped = json.decodeFromString<CoOccurrenceWithContext>(json.encodeToString(emptyOrder))

        assertThat(roundTripped).isEqualTo(emptyOrder)
    }
}
