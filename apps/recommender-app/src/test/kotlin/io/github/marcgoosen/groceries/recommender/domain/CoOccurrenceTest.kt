package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * Counts of what was bought alongside a product, kept in a changelog-backed store.
 */
class CoOccurrenceTest {
    private val json = Json { prettyPrint = true }

    private val coOccurrence = CoOccurrence(mapOf("p1" to 2, "p2" to 1))

    private val empty = CoOccurrence()

    @Test
    fun `It should serialize a populated co-occurrence to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(coOccurrence)

        assertThat(serialized).isEqualTo(
            """
            {
                "countsByProduct": {
                    "p1": 2,
                    "p2": 1
                }
            }
            """.trimIndent(),
        )
        assertThat(json.decodeFromString<CoOccurrence>(serialized)).isEqualTo(coOccurrence)
    }

    @Test
    fun `It should serialize a populated co-occurrence to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(CoOccurrence.serializer(), coOccurrence)

        assertThat(serialized.toHex()).isEqualTo("04047031040470320200")
        assertThat(Avro.decodeFromByteArray(CoOccurrence.serializer(), serialized)).isEqualTo(coOccurrence)
    }

    @Test
    fun `It should serialize its defaults to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(empty)

        assertThat(serialized).isEqualTo(
            """
            {}
            """.trimIndent(),
        )
        assertThat(json.decodeFromString<CoOccurrence>(serialized)).isEqualTo(empty)
    }

    @Test
    fun `It should serialize its defaults to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(CoOccurrence.serializer(), empty)

        assertThat(serialized.toHex()).isEqualTo("00")
        assertThat(Avro.decodeFromByteArray(CoOccurrence.serializer(), serialized)).isEqualTo(empty)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
