package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * Counts of what was bought alongside a product. Written to a changelog, so both encodings must survive.
 */
class CoOccurrenceTest {
    private val json = Json { prettyPrint = true }

    private val coOccurrence = CoOccurrence(mapOf("p1" to 2, "p2" to 1))

    @Test
    fun `It should round-trip a populated co-occurrence through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<CoOccurrence>(json.encodeToString(coOccurrence))

        assertThat(roundTripped).isEqualTo(coOccurrence)
    }

    @Test
    fun `It should round-trip a populated co-occurrence through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            CoOccurrence.serializer(),
            Avro.encodeToByteArray(CoOccurrence.serializer(), coOccurrence),
        )

        assertThat(roundTripped).isEqualTo(coOccurrence)
    }

    @Test
    fun `It should omit its defaults from the JSON payload and read them back`() {
        // Given
        val defaults = CoOccurrence()

        // When
        val serialized = json.encodeToString(defaults)

        assertThat(serialized).isEqualTo("{}")
        assertThat(json.decodeFromString<CoOccurrence>(serialized)).isEqualTo(defaults)
    }

    @Test
    fun `It should round-trip a default-valued instance through Avro`() {
        // Given
        val defaults = CoOccurrence()

        // When
        val roundTripped = Avro.decodeFromByteArray(
            CoOccurrence.serializer(),
            Avro.encodeToByteArray(CoOccurrence.serializer(), defaults),
        )

        assertThat(roundTripped).isEqualTo(defaults)
    }
}
