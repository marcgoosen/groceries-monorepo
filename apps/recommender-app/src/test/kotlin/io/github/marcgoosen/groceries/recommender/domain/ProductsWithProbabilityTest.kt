package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import io.github.marcgoosen.groceries.shared.domain.Product
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * The recommendation that leaves the pipeline on the related-products topic.
 */
class ProductsWithProbabilityTest {
    private val json = Json { prettyPrint = true }

    private val productsWithProbability = ProductsWithProbability(
        listOf(
            ProductWithProbability(Product("p1", "Milk", 2.0), 0.5),
            ProductWithProbability(Product("p2", "Bread", 1.5), 0.3),
        ),
    )

    @Test
    fun `It should round-trip a full recommendation through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<ProductsWithProbability>(json.encodeToString(productsWithProbability))

        assertThat(roundTripped).isEqualTo(productsWithProbability)
    }

    @Test
    fun `It should round-trip a full recommendation through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            ProductsWithProbability.serializer(),
            Avro.encodeToByteArray(ProductsWithProbability.serializer(), productsWithProbability),
        )

        assertThat(roundTripped).isEqualTo(productsWithProbability)
    }

    @Test
    fun `It should round-trip a recommendation with no products left`() {
        // Given
        val empty = ProductsWithProbability(emptyList())

        // When
        val roundTripped = json.decodeFromString<ProductsWithProbability>(json.encodeToString(empty))

        assertThat(roundTripped).isEqualTo(empty)
    }
}
