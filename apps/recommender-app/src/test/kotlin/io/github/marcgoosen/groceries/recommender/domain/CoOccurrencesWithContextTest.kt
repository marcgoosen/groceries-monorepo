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
 * The fan-in aggregate. Both fields are defaulted, so it starts empty on the first update.
 */
class CoOccurrencesWithContextTest {
    private val json = Json { prettyPrint = true }

    private val order = Order(
        orderId = "order-1",
        orderLines = listOf(OrderLine("p1", 2.0, 1), OrderLine("p2", 1.5, 3)),
        timestamp = Instant.parse("2026-01-15T09:30:00.123Z"),
    )

    private val context = CoOccurrencesWithContext(
        coOccurrences = listOf(CoOccurrence(mapOf("p3" to 1)), CoOccurrence(mapOf("p4" to 2))),
        order = order,
    )

    private val empty = CoOccurrencesWithContext()

    @Test
    fun `It should serialize a populated aggregate to JSON and read it back`() {
        // Given
        // When
        val serialized = json.encodeToString(context)

        assertThat(serialized).isEqualTo(
            """
            {
                "coOccurrences": [
                    {
                        "countsByProduct": {
                            "p3": 1
                        }
                    },
                    {
                        "countsByProduct": {
                            "p4": 2
                        }
                    }
                ],
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
        assertThat(json.decodeFromString<CoOccurrencesWithContext>(serialized)).isEqualTo(context)
    }

    @Test
    fun `It should serialize a populated aggregate to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(CoOccurrencesWithContext.serializer(), context)

        assertThat(
            serialized.toHex(),
        ).isEqualTo(
            "0402047033020002047034040000020e6f726465722d3104047031000000000000004002047032000000000000f83f0600f6a8ec8ff866",
        )
        assertThat(Avro.decodeFromByteArray(CoOccurrencesWithContext.serializer(), serialized)).isEqualTo(context)
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
        assertThat(json.decodeFromString<CoOccurrencesWithContext>(serialized)).isEqualTo(empty)
    }

    @Test
    fun `It should serialize its defaults to Avro and read it back`() {
        // Given
        // When
        val serialized = Avro.encodeToByteArray(CoOccurrencesWithContext.serializer(), empty)

        assertThat(serialized.toHex()).isEqualTo("0000")
        assertThat(Avro.decodeFromByteArray(CoOccurrencesWithContext.serializer(), serialized)).isEqualTo(empty)
    }
}

private fun ByteArray.toHex() = joinToString("") { "%02x".format(it) }
