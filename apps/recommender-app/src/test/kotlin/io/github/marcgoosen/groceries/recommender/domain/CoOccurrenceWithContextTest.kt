package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import kotlinx.datetime.Instant
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test

/**
 * One product's co-occurrences carried together with the order that triggered the lookup.
 */
class CoOccurrenceWithContextTest {
    private val json = Json { prettyPrint = true }

    private val order = Order(
        orderId = "order-1",
        orderLines = listOf(OrderLine("p1", 2.0, 1), OrderLine("p2", 1.5, 3)),
        timestamp = Instant.parse("2026-01-15T09:30:00.123Z"),
    )

    private val context = CoOccurrenceWithContext(CoOccurrence(mapOf("p3" to 4)), order)

    @Test
    fun `It should serialize a populated context to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(context)

        assertThat(serialized).isEqualTo(
            """
            {
                "coOccurrence": {
                    "countsByProduct": {
                        "p3": 4
                    }
                },
                "order": {
                    "orderId": "order-1",
                    "orderLines": [
                        {
                            "productId": "p1",
                            "price": 2.0,
                            "quantity": 1
                        },
                        {
                            "productId": "p2",
                            "price": 1.5,
                            "quantity": 3
                        }
                    ],
                    "timestamp": 1768469400123
                }
            }
            """.trimIndent(),
        )
        assertThat(json.decodeFromString<CoOccurrenceWithContext>(serialized)).isEqualTo(context)
    }

    @Test
    fun `It should serialize a populated context to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(CoOccurrenceWithContext.serializer(), context)

        assertThat(
            serialized.toHex(),
        ).isEqualTo("0204703308000e6f726465722d3104047031000000000000004002047032000000000000f83f0600f6a8ec8ff866")
        assertThat(Avro.decodeFromByteArray(CoOccurrenceWithContext.serializer(), serialized)).isEqualTo(context)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
