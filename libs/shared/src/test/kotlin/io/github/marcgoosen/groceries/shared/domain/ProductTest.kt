package io.github.marcgoosen.groceries.shared.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import org.junit.jupiter.api.Test

/**
 * Product is the compacted lookup topic the pipeline joins against, so its Avro encoding is the contract with
 * whoever maintains the catalogue.
 */
class ProductTest {
    private val milk = Product(productId = "p1", name = "Milk", price = 2.0)

    @Test
    fun `It should round-trip through Avro`() {
        // Given
        // When
        val roundTripped = Avro.decodeFromByteArray(
            Product.serializer(),
            Avro.encodeToByteArray(Product.serializer(), milk),
        )

        assertThat(roundTripped).isEqualTo(milk)
    }

    @Test
    fun `It should round-trip a product whose name needs more than ASCII`() {
        // Given
        val product = milk.copy(productId = "p2", name = "Crème fraîche 30%")

        // When
        val roundTripped = Avro.decodeFromByteArray(
            Product.serializer(),
            Avro.encodeToByteArray(Product.serializer(), product),
        )

        assertThat(roundTripped).isEqualTo(product)
    }
}
