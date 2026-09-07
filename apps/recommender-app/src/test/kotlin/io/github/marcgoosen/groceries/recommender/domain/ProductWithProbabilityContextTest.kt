package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import io.github.marcgoosen.groceries.shared.domain.Product
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * A looked-up product plus the order it is destined for. The product is null when the join found no catalogue entry,
 * so that path has to survive both encodings.
 */
class ProductWithProbabilityContextTest {
    private val json = Json { prettyPrint = true }

    private val context = ProductWithProbabilityContext(
        productWithProbability = ProductWithProbability(Product("p1", "Milk", 2.0), 0.5),
        orderId = "order-1",
        expectedSize = 3,
    )

    @Test
    fun `It should round-trip a context with a product through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<ProductWithProbabilityContext>(json.encodeToString(context))

        assertThat(roundTripped).isEqualTo(context)
    }

    @Test
    fun `It should round-trip a context with a product through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            ProductWithProbabilityContext.serializer(),
            Avro.encodeToByteArray(ProductWithProbabilityContext.serializer(), context),
        )

        assertThat(roundTripped).isEqualTo(context)
    }

    @Test
    fun `It should round-trip a context whose product was not found through JSON`() {
        // Given
        val unresolved = context.copy(productWithProbability = null)

        // When
        val roundTripped = json.decodeFromString<ProductWithProbabilityContext>(json.encodeToString(unresolved))

        assertThat(roundTripped).isEqualTo(unresolved)
    }

    @Test
    fun `It should round-trip a context whose product was not found through Avro`() {
        // Given
        val unresolved = context.copy(productWithProbability = null)

        // When
        val roundTripped = Avro.decodeFromByteArray(
            ProductWithProbabilityContext.serializer(),
            Avro.encodeToByteArray(ProductWithProbabilityContext.serializer(), unresolved),
        )

        assertThat(roundTripped).isEqualTo(unresolved)
    }
}
