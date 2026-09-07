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
    fun `It should round-trip a probability context through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<ProbabilityContext>(json.encodeToString(probabilityContext))

        assertThat(roundTripped).isEqualTo(probabilityContext)
    }

    @Test
    fun `It should round-trip a probability context through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            ProbabilityContext.serializer(),
            Avro.encodeToByteArray(ProbabilityContext.serializer(), probabilityContext),
        )

        assertThat(roundTripped).isEqualTo(probabilityContext)
    }
}
