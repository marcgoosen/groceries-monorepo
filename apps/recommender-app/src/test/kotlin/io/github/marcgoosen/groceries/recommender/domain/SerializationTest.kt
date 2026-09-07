package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import io.github.marcgoosen.groceries.shared.domain.Product
import kotlinx.datetime.Instant
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * These models cross the wire as Avro, but kotlinx-serialization generates a defaulted-field branch per property in
 * both directions. An all-defaults round-trip exercises the branches a fully populated payload never reaches.
 */
class SerializationTest {
    private val json = Json { prettyPrint = true }

    private val milk = Product("p1", "Milk", 2.0)

    private val order = Order(
        orderId = "o1",
        orderLines = listOf(OrderLine("p1", 2.0, 1)),
        timestamp = Instant.parse("2026-01-15T09:30:00Z"),
    )

    @Test
    fun `It should round-trip a co-occurrence through the empty-object payload`() {
        // Given
        val empty = CoOccurrence()

        // When
        val serialized = json.encodeToString(empty)

        assertThat(serialized).isEqualTo("{}")
        assertThat(json.decodeFromString<CoOccurrence>(serialized)).isEqualTo(empty)
    }

    @Test
    fun `It should round-trip a co-distribution through the empty-object payload`() {
        // Given
        val empty = CoDistribution()

        // When
        val serialized = json.encodeToString(empty)

        assertThat(serialized).isEqualTo("{}")
        assertThat(json.decodeFromString<CoDistribution>(serialized)).isEqualTo(empty)
    }

    @Test
    fun `It should round-trip co-occurrences without an order through the empty-object payload`() {
        // Given
        val empty = CoOccurrencesWithContext()

        // When
        val serialized = json.encodeToString(empty)

        assertThat(serialized).isEqualTo("{}")
        assertThat(json.decodeFromString<CoOccurrencesWithContext>(serialized)).isEqualTo(empty)
    }

    @Test
    fun `It should round-trip an empty products context through the empty-object payload`() {
        // Given
        val empty = ProductsWithProbabilityContext()

        // When
        val serialized = json.encodeToString(empty)

        assertThat(serialized).isEqualTo("{}")
        assertThat(json.decodeFromString<ProductsWithProbabilityContext>(serialized)).isEqualTo(empty)
    }

    @Test
    fun `It should round-trip a fully populated co-occurrences aggregate through Avro`() {
        // Given
        val context = CoOccurrencesWithContext(
            coOccurrences = listOf(CoOccurrence(mapOf("p2" to 3))),
            order = order,
        )

        // When
        val roundTripped = Avro.decodeFromByteArray(
            CoOccurrencesWithContext.serializer(),
            Avro.encodeToByteArray(CoOccurrencesWithContext.serializer(), context),
        )

        assertThat(roundTripped).isEqualTo(context)
    }

    @Test
    fun `It should round-trip a fully populated products context`() {
        // Given
        val context = ProductsWithProbabilityContext(
            productsWithProbabilities = listOf(ProductWithProbability(milk, 0.5), null),
            expectedSize = 2,
        )

        // When
        val roundTripped = json.decodeFromString<ProductsWithProbabilityContext>(json.encodeToString(context))

        assertThat(roundTripped).isEqualTo(context)
    }

    @Test
    fun `It should round-trip the recommendation that leaves the pipeline`() {
        // Given
        val recommendation = ProductsWithProbability(listOf(ProductWithProbability(milk, 0.5)))

        // When
        val roundTripped = json.decodeFromString<ProductsWithProbability>(json.encodeToString(recommendation))

        assertThat(roundTripped).isEqualTo(recommendation)
    }

    @Test
    fun `It should round-trip the probability context carried between stages`() {
        // Given
        val context = ProbabilityContext(0.75, "o1", 3)

        // When
        val roundTripped = json.decodeFromString<ProbabilityContext>(json.encodeToString(context))

        assertThat(roundTripped).isEqualTo(context)
    }
}
