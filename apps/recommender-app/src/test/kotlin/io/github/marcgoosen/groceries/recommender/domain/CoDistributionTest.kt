package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * Co-occurrence counts turned into probabilities.
 */
class CoDistributionTest {
    private val json = Json { prettyPrint = true }

    private val coDistribution = CoDistribution(mapOf("p1" to 0.4, "p2" to 0.6))

    private val empty = CoDistribution()

    @Test
    fun `It should serialize a populated distribution to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(CoDistribution.serializer(), coDistribution)

        assertThat(serialized).isEqualTo(
            """
            {
                "probabilityByProductId": {
                    "p1": 0.4,
                    "p2": 0.6
                }
            }
            """.trimIndent(),
        )
        assertThat(json.decodeFromString(CoDistribution.serializer(), serialized)).isEqualTo(coDistribution)
    }

    @Test
    fun `It should serialize a populated distribution to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(CoDistribution.serializer(), coDistribution)

        assertThat(serialized.toHex()).isEqualTo("040470319a9999999999d93f047032333333333333e33f00")
        assertThat(Avro.decodeFromByteArray(CoDistribution.serializer(), serialized)).isEqualTo(coDistribution)
    }

    @Test
    fun `It should serialize its defaults to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(CoDistribution.serializer(), empty)

        assertThat(serialized).isEqualTo(
            """
            {}
            """.trimIndent(),
        )
        assertThat(json.decodeFromString(CoDistribution.serializer(), serialized)).isEqualTo(empty)
    }

    @Test
    fun `It should serialize its defaults to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(CoDistribution.serializer(), empty)

        assertThat(serialized.toHex()).isEqualTo("00")
        assertThat(Avro.decodeFromByteArray(CoDistribution.serializer(), serialized)).isEqualTo(empty)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
