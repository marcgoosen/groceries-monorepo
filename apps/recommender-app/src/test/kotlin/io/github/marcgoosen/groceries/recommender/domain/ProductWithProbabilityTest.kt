package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import io.github.marcgoosen.groceries.shared.domain.Product
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * A recommended product and how likely it is; the element of what the pipeline finally emits.
 */
class ProductWithProbabilityTest {
    private val json = Json { prettyPrint = true }

    private val productWithProbability = ProductWithProbability(Product("p1", "Milk", 2.0), 0.5)

    @Test
    fun `It should round-trip a product with its probability through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<ProductWithProbability>(json.encodeToString(productWithProbability))

        assertThat(roundTripped).isEqualTo(productWithProbability)
    }

    @Test
    fun `It should round-trip a product with its probability through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            ProductWithProbability.serializer(),
            Avro.encodeToByteArray(ProductWithProbability.serializer(), productWithProbability),
        )

        assertThat(roundTripped).isEqualTo(productWithProbability)
    }
}
