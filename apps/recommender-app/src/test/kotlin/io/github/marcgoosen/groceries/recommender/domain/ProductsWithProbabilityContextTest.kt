package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import io.github.marcgoosen.groceries.shared.domain.Product
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * The second fan-in aggregate. Both fields are defaulted, and the list holds nulls for unresolved products.
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

    private val empty = ProductsWithProbabilityContext()

    @Test
    fun `It should serialize a populated aggregate to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(context)

        assertThat(serialized).isEqualTo(
            """
            {
                "productsWithProbabilities": [
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
                ],
                "expectedSize": 2
            }
            """.trimIndent(),
        )
        assertThat(json.decodeFromString<ProductsWithProbabilityContext>(serialized)).isEqualTo(context)
    }

    @Test
    fun `It should serialize a populated aggregate to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(ProductsWithProbabilityContext.serializer(), context)

        assertThat(
            serialized.toHex(),
        ).isEqualTo(
            "0402047031084d696c6b0000000000000040000000000000e03f020470320a4272656164000000000000f83f333333333333d33f0004",
        )
        assertThat(Avro.decodeFromByteArray(ProductsWithProbabilityContext.serializer(), serialized)).isEqualTo(context)
    }

    @Test
    fun `It should serialize its defaults to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(empty)

        assertThat(serialized).isEqualTo(
            """
            {}
            """.trimIndent(),
        )
        assertThat(json.decodeFromString<ProductsWithProbabilityContext>(serialized)).isEqualTo(empty)
    }

    @Test
    fun `It should serialize its defaults to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(ProductsWithProbabilityContext.serializer(), empty)

        assertThat(serialized.toHex()).isEqualTo("0000")
        assertThat(Avro.decodeFromByteArray(ProductsWithProbabilityContext.serializer(), serialized)).isEqualTo(empty)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
