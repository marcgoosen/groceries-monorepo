package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import io.github.marcgoosen.groceries.shared.domain.Product
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * A recommended product and how likely it is.
 */
class RelatedProductTest {
    private val json = Json { prettyPrint = true }

    private val relatedProduct = RelatedProduct(Product("p1", "Milk", 2.0), 0.5)

    @Test
    fun `It should serialize a product with its probability to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(relatedProduct)

        assertThat(serialized).isEqualTo(
            """
            {
                "product": {
                    "productId": "p1",
                    "name": "Milk",
                    "price": 2.0
                },
                "probability": 0.5
            }
            """.trimIndent(),
        )
        assertThat(
            json.decodeFromString<RelatedProduct>(serialized),
        ).isEqualTo(relatedProduct)
    }

    @Test
    fun `It should serialize a product with its probability to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(RelatedProduct.serializer(), relatedProduct)

        assertThat(serialized.toHex()).isEqualTo("047031084d696c6b0000000000000040000000000000e03f")
        assertThat(
            Avro.decodeFromByteArray(RelatedProduct.serializer(), serialized),
        ).isEqualTo(relatedProduct)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
