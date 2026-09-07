package io.github.marcgoosen.groceries.shared.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * Product is the compacted lookup topic the pipeline joins against.
 */
class ProductTest {
    private val json = Json { prettyPrint = true }

    private val product = Product(productId = "p1", name = "Milk", price = 2.0)

    @Test
    fun `It should serialize a product to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(Product.serializer(), product)

        assertThat(serialized).isEqualTo(
            """
            {
                "productId": "p1",
                "name": "Milk",
                "price": 2.0
            }
            """.trimIndent(),
        )
        assertThat(json.decodeFromString(Product.serializer(), serialized)).isEqualTo(product)
    }

    @Test
    fun `It should serialize a product to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(Product.serializer(), product)

        assertThat(serialized.toHex()).isEqualTo("047031084d696c6b0000000000000040")
        assertThat(Avro.decodeFromByteArray(Product.serializer(), serialized)).isEqualTo(product)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
