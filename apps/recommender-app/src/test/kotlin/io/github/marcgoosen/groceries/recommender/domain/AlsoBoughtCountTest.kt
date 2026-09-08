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
class AlsoBoughtCountTest {
    private val json = Json { prettyPrint = true }

    private val alsoBought = AlsoBoughtCount(mapOf("p1" to 2, "p2" to 1))

    private val empty = AlsoBoughtCount()

    @Test
    fun `It should serialize a populated also-bought to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(alsoBought)

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
        assertThat(json.decodeFromString<AlsoBoughtCount>(serialized)).isEqualTo(alsoBought)
    }

    @Test
    fun `It should serialize a populated also-bought to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(AlsoBoughtCount.serializer(), alsoBought)

        assertThat(serialized.toHex()).isEqualTo("04047031040470320200")
        assertThat(Avro.decodeFromByteArray(AlsoBoughtCount.serializer(), serialized)).isEqualTo(alsoBought)
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
        assertThat(json.decodeFromString<AlsoBoughtCount>(serialized)).isEqualTo(empty)
    }

    @Test
    fun `It should serialize its defaults to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(AlsoBoughtCount.serializer(), empty)

        assertThat(serialized.toHex()).isEqualTo("00")
        assertThat(Avro.decodeFromByteArray(AlsoBoughtCount.serializer(), serialized)).isEqualTo(empty)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
