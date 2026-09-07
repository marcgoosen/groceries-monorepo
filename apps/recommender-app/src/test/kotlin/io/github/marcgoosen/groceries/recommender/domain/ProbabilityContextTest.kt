package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * One candidate product's probability plus the order it belongs to, keyed by product between the two joins.
 */
class ProbabilityContextTest {
    private val json = Json { prettyPrint = true }

    private val probabilityContext = ProbabilityContext(probability = 0.75, orderId = "order-1", expectedSize = 3)

    @Test
    fun `It should serialize a probability context to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(probabilityContext)

        assertThat(serialized).isEqualTo(
            """
            {
                "probability": 0.75,
                "orderId": "order-1",
                "expectedSize": 3
            }
            """.trimIndent(),
        )
        assertThat(json.decodeFromString<ProbabilityContext>(serialized)).isEqualTo(probabilityContext)
    }

    @Test
    fun `It should serialize a probability context to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(ProbabilityContext.serializer(), probabilityContext)

        assertThat(serialized.toHex()).isEqualTo("000000000000e83f0e6f726465722d3106")
        assertThat(Avro.decodeFromByteArray(ProbabilityContext.serializer(), serialized)).isEqualTo(probabilityContext)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
