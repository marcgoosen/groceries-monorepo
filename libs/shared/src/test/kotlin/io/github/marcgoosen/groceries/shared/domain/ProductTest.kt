package io.github.marcgoosen.groceries.shared.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * Product is the compacted lookup topic the pipeline joins against, so its Avro encoding is the contract with
 * whoever maintains the catalogue.
 */
class ProductTest {
    private val json = Json { prettyPrint = true }

    private val product = Product(productId = "p1", name = "Milk", price = 2.0)

    @Test
    fun `It should round-trip a product through JSON`() {
        // Given
        // When
        val roundTripped = json.decodeFromString<Product>(json.encodeToString(product))

        assertThat(roundTripped).isEqualTo(product)
    }

    @Test
    fun `It should round-trip a product through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            Product.serializer(),
            Avro.encodeToByteArray(Product.serializer(), product),
        )

        assertThat(roundTripped).isEqualTo(product)
    }

    @Test
    fun `It should round-trip a product whose name needs more than ASCII`() {
        // Given
        val accented = product.copy(productId = "p2", name = "Crème fraîche 30%")

        // When
        val roundTripped = Avro.decodeFromByteArray(
            Product.serializer(),
            Avro.encodeToByteArray(Product.serializer(), accented),
        )

        assertThat(roundTripped).isEqualTo(accented)
    }
}
