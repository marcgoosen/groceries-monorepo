package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * Co-occurrence counts expressed as probabilities, carried between pipeline stages.
 */
class CoDistributionTest {
    private val json = Json { prettyPrint = true }

    private val coDistribution = CoDistribution(mapOf("p1" to 0.4, "p2" to 0.6))

    @Test
    fun `It should round-trip a populated distribution through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<CoDistribution>(json.encodeToString(coDistribution))

        assertThat(roundTripped).isEqualTo(coDistribution)
    }

    @Test
    fun `It should round-trip a populated distribution through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            CoDistribution.serializer(),
            Avro.encodeToByteArray(CoDistribution.serializer(), coDistribution),
        )

        assertThat(roundTripped).isEqualTo(coDistribution)
    }

    @Test
    fun `It should omit its defaults from the JSON payload and read them back`() {
        // Given
        val defaults = CoDistribution()

        // When
        val serialized = json.encodeToString(defaults)

        assertThat(serialized).isEqualTo("{}")
        assertThat(json.decodeFromString<CoDistribution>(serialized)).isEqualTo(defaults)
    }

    @Test
    fun `It should round-trip a default-valued instance through Avro`() {
        // Given
        val defaults = CoDistribution()

        // When
        val roundTripped = Avro.decodeFromByteArray(
            CoDistribution.serializer(),
            Avro.encodeToByteArray(CoDistribution.serializer(), defaults),
        )

        assertThat(roundTripped).isEqualTo(defaults)
    }
}
