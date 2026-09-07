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
class RelatedProductsTest {
    private val json = Json { prettyPrint = true }

    private val relatedProducts = RelatedProducts(
        listOf(
            RelatedProduct(Product("p1", "Milk", 2.0), 0.5),
            RelatedProduct(Product("p2", "Bread", 1.5), 0.3),
        ),
    )

    @Test
    fun `It should serialize a full recommendation to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(relatedProducts)

        assertThat(serialized).isEqualTo(
            """
            {
                "relatedProducts": [
                    {
                        "product": {
                            "productId": "p1",
                            "name": "Milk",
                            "price": 2.0
                        },
                        "probability": 0.5
                    },
                    {
                        "product": {
                            "productId": "p2",
                            "name": "Bread",
                            "price": 1.5
                        },
                        "probability": 0.3
                    }
                ]
            }
            """.trimIndent(),
        )
        assertThat(
            json.decodeFromString<RelatedProducts>(serialized),
        ).isEqualTo(relatedProducts)
    }

    @Test
    fun `It should serialize a full recommendation to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(RelatedProducts.serializer(), relatedProducts)

        assertThat(
            serialized.toHex(),
        ).isEqualTo(
            "04047031084d696c6b0000000000000040000000000000e03f0470320a4272656164000000000000f83f333333333333d33f00",
        )
        assertThat(
            Avro.decodeFromByteArray(RelatedProducts.serializer(), serialized),
        ).isEqualTo(relatedProducts)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
