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
 * The fan-in aggregate: every ordered product's co-occurrences gathered back under one order. Both fields are
 * defaulted, so the aggregate starts empty on the very first update.
 */
class CoOccurrencesWithContextTest {
    private val json = Json { prettyPrint = true }

    private val order = Order(
        orderId = "order-1",
        orderLines = listOf(OrderLine("p1", 2.0, 1), OrderLine("p2", 1.5, 3)),
        timestamp = Instant.parse("2026-01-15T09:30:00.123Z"),
    )

    private val context = CoOccurrencesWithContext(
        coOccurrences = listOf(CoOccurrence(mapOf("p3" to 1)), CoOccurrence(mapOf("p4" to 2))),
        order = order,
    )

    @Test
    fun `It should round-trip a populated aggregate through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<CoOccurrencesWithContext>(json.encodeToString(context))

        assertThat(roundTripped).isEqualTo(context)
    }

    @Test
    fun `It should round-trip a populated aggregate through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            CoOccurrencesWithContext.serializer(),
            Avro.encodeToByteArray(CoOccurrencesWithContext.serializer(), context),
        )

        assertThat(roundTripped).isEqualTo(context)
    }

    @Test
    fun `It should omit its defaults from the JSON payload and read them back`() {
        // Given
        val defaults = CoOccurrencesWithContext()

        // When
        val serialized = json.encodeToString(defaults)

        assertThat(serialized).isEqualTo("{}")
        assertThat(json.decodeFromString<CoOccurrencesWithContext>(serialized)).isEqualTo(defaults)
    }

    @Test
    fun `It should round-trip a default-valued instance through Avro`() {
        // Given
        val defaults = CoOccurrencesWithContext()

        // When
        val roundTripped = Avro.decodeFromByteArray(
            CoOccurrencesWithContext.serializer(),
            Avro.encodeToByteArray(CoOccurrencesWithContext.serializer(), defaults),
        )

        assertThat(roundTripped).isEqualTo(defaults)
    }

    @Test
    fun `It should round-trip an aggregate that has an order but no co-occurrences yet`() {
        // Given
        val started = CoOccurrencesWithContext(order = order)

        // When
        val roundTripped = Avro.decodeFromByteArray(
            CoOccurrencesWithContext.serializer(),
            Avro.encodeToByteArray(CoOccurrencesWithContext.serializer(), started),
        )

        assertThat(roundTripped).isEqualTo(started)
    }
}
