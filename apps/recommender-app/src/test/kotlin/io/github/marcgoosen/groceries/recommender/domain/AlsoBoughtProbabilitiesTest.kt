package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * Also-bought counts turned into probabilities.
 */
class AlsoBoughtProbabilitiesTest {
    private val json = Json { prettyPrint = true }

    private val alsoBoughtProbabilities = AlsoBoughtProbabilities(mapOf("p1" to 0.4, "p2" to 0.6))

    private val empty = AlsoBoughtProbabilities()

    @Test
    fun `It should serialize a populated distribution to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(alsoBoughtProbabilities)

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
        assertThat(
            json.decodeFromString<AlsoBoughtProbabilities>(serialized),
        ).isEqualTo(alsoBoughtProbabilities)
    }

    @Test
    fun `It should serialize a populated distribution to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(AlsoBoughtProbabilities.serializer(), alsoBoughtProbabilities)

        assertThat(serialized.toHex()).isEqualTo("040470319a9999999999d93f047032333333333333e33f00")
        assertThat(
            Avro.decodeFromByteArray(AlsoBoughtProbabilities.serializer(), serialized),
        ).isEqualTo(alsoBoughtProbabilities)
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
        assertThat(json.decodeFromString<AlsoBoughtProbabilities>(serialized)).isEqualTo(empty)
    }

    @Test
    fun `It should serialize its defaults to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(AlsoBoughtProbabilities.serializer(), empty)

        assertThat(serialized.toHex()).isEqualTo("00")
        assertThat(Avro.decodeFromByteArray(AlsoBoughtProbabilities.serializer(), serialized)).isEqualTo(empty)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
