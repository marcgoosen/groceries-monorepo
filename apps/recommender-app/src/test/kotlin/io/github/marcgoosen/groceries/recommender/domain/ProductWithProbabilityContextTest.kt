package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import io.github.marcgoosen.groceries.shared.domain.Product
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * A looked-up product plus the order it is destined for. The product is null when the join found no catalogue entry.
 */
class ProductWithProbabilityContextTest {
    private val json = Json { prettyPrint = true }

    private val context = ProductWithProbabilityContext(
        productWithProbability = ProductWithProbability(Product("p1", "Milk", 2.0), 0.5),
        orderId = "order-1",
        expectedSize = 3,
    )

    private val unresolved = ProductWithProbabilityContext(null, "order-1", 3)

    @Test
    fun `It should serialize a context with a product to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(ProductWithProbabilityContext.serializer(), context)

        assertThat(serialized).isEqualTo(
            """
            {
                "productWithProbability": {
                    "product": {
                        "productId": "p1",
                        "name": "Milk",
                        "price": 2.0
                    },
                    "probability": 0.5
                },
                "orderId": "order-1",
                "expectedSize": 3
            }
            """.trimIndent(),
        )
        assertThat(json.decodeFromString(ProductWithProbabilityContext.serializer(), serialized)).isEqualTo(context)
    }

    @Test
    fun `It should serialize a context with a product to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(ProductWithProbabilityContext.serializer(), context)

        assertThat(serialized.toHex()).isEqualTo("02047031084d696c6b0000000000000040000000000000e03f0e6f726465722d3106")
        assertThat(Avro.decodeFromByteArray(ProductWithProbabilityContext.serializer(), serialized)).isEqualTo(context)
    }

    @Test
    fun `It should serialize a context whose product was not found to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(ProductWithProbabilityContext.serializer(), unresolved)

        assertThat(serialized).isEqualTo(
            """
            {
                "productWithProbability": null,
                "orderId": "order-1",
                "expectedSize": 3
            }
            """.trimIndent(),
        )
        assertThat(json.decodeFromString(ProductWithProbabilityContext.serializer(), serialized)).isEqualTo(unresolved)
    }

    @Test
    fun `It should serialize a context whose product was not found to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(ProductWithProbabilityContext.serializer(), unresolved)

        assertThat(serialized.toHex()).isEqualTo("000e6f726465722d3106")
        assertThat(
            Avro.decodeFromByteArray(ProductWithProbabilityContext.serializer(), serialized),
        ).isEqualTo(unresolved)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
