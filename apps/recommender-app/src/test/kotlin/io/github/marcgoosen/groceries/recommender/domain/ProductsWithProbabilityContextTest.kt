package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import io.github.marcgoosen.groceries.shared.domain.Product
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * The second fan-in aggregate. Both fields are defaulted so it starts empty, and the list holds nulls for products
 * the join could not resolve.
 */
class ProductsWithProbabilityContextTest {
    private val json = Json { prettyPrint = true }

    private val context = ProductsWithProbabilityContext(
        productsWithProbabilities = listOf(
            ProductWithProbability(Product("p1", "Milk", 2.0), 0.5),
            ProductWithProbability(Product("p2", "Bread", 1.5), 0.3),
        ),
        expectedSize = 2,
    )

    @Test
    fun `It should round-trip a populated aggregate through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<ProductsWithProbabilityContext>(json.encodeToString(context))

        assertThat(roundTripped).isEqualTo(context)
    }

    @Test
    fun `It should round-trip a populated aggregate through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            ProductsWithProbabilityContext.serializer(),
            Avro.encodeToByteArray(ProductsWithProbabilityContext.serializer(), context),
        )

        assertThat(roundTripped).isEqualTo(context)
    }

    @Test
    fun `It should omit its defaults from the JSON payload and read them back`() {
        // Given
        val defaults = ProductsWithProbabilityContext()

        // When
        val serialized = json.encodeToString(defaults)

        assertThat(serialized).isEqualTo("{}")
        assertThat(json.decodeFromString<ProductsWithProbabilityContext>(serialized)).isEqualTo(defaults)
    }

    @Test
    fun `It should round-trip a default-valued instance through Avro`() {
        // Given
        val defaults = ProductsWithProbabilityContext()

        // When
        val roundTripped = Avro.decodeFromByteArray(
            ProductsWithProbabilityContext.serializer(),
            Avro.encodeToByteArray(ProductsWithProbabilityContext.serializer(), defaults),
        )

        assertThat(roundTripped).isEqualTo(defaults)
    }

    @Test
    fun `It should round-trip an aggregate holding a product that was not found`() {
        // Given
        val withUnresolved = context.copy(productsWithProbabilities = context.productsWithProbabilities + null)

        // When
        val roundTripped = Avro.decodeFromByteArray(
            ProductsWithProbabilityContext.serializer(),
            Avro.encodeToByteArray(ProductsWithProbabilityContext.serializer(), withUnresolved),
        )

        assertThat(roundTripped).isEqualTo(withUnresolved)
    }
}
