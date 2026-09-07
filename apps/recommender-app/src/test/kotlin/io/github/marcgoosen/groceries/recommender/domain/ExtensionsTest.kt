package io.github.marcgoosen.groceries.recommender.domain

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isNull
import assertk.assertions.isTrue
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import io.github.marcgoosen.groceries.shared.domain.Product
import kotlinx.datetime.Instant
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class ExtensionsTest {
    private val milk = Product("p1", "Milk", 2.0)
    private val bread = Product("p2", "Bread", 1.5)
    private val eggs = Product("p3", "Eggs", 3.0)

    private val order = Order(
        orderId = "o1",
        timestamp = Instant.parse("2026-01-15T09:30:00Z"),
        orderLines = listOf(
            OrderLine(milk.productId, milk.price, 1),
            OrderLine(bread.productId, bread.price, 1),
        ),
    )

    @Nested
    inner class ProductIds {
        @Test
        fun `It should list every product in the order without duplicates`() {
            // Given
            val orderWithDuplicate = order.copy(
                orderLines = order.orderLines + OrderLine(milk.productId, milk.price, 2),
            )

            // When
            val productIds = orderWithDuplicate.productIds

            assertThat(productIds).isEqualTo(setOf(milk.productId, bread.productId))
        }
    }

    @Nested
    inner class TotalCount {
        @Test
        fun `It should sum the counts across all co-occurring products`() {
            // Given
            val coOccurrence = CoOccurrence(mapOf(milk.productId to 2, bread.productId to 3))

            // When
            val totalCount = coOccurrence.totalCount

            assertThat(totalCount).isEqualTo(5)
        }

        @Test
        fun `It should be zero when nothing co-occurred`() {
            // Given
            val coOccurrence = CoOccurrence()

            // When
            val totalCount = coOccurrence.totalCount

            assertThat(totalCount).isEqualTo(0)
        }
    }

    @Nested
    inner class CoOccurrencePlusProductId {
        @Test
        fun `It should start a count for a product seen for the first time`() {
            // Given
            val coOccurrence = CoOccurrence()

            // When
            val incremented = coOccurrence + milk.productId

            assertThat(incremented).isEqualTo(CoOccurrence(mapOf(milk.productId to 1)))
        }

        @Test
        fun `It should raise the count of a product seen before`() {
            // Given
            val coOccurrence = CoOccurrence(mapOf(milk.productId to 2))

            // When
            val incremented = coOccurrence + milk.productId

            assertThat(incremented).isEqualTo(CoOccurrence(mapOf(milk.productId to 3)))
        }
    }

    @Nested
    inner class CoOccurrenceMinus {
        @Test
        fun `It should drop a single product`() {
            // Given
            val coOccurrence = CoOccurrence(mapOf(milk.productId to 2, bread.productId to 1))

            // When
            val remaining = coOccurrence - milk.productId

            assertThat(remaining).isEqualTo(CoOccurrence(mapOf(bread.productId to 1)))
        }

        @Test
        fun `It should drop every product in the given set`() {
            // Given
            val coOccurrence = CoOccurrence(mapOf(milk.productId to 2, bread.productId to 1, eggs.productId to 4))

            // When
            val remaining = coOccurrence - setOf(milk.productId, bread.productId)

            assertThat(remaining).isEqualTo(CoOccurrence(mapOf(eggs.productId to 4)))
        }
    }

    @Nested
    inner class CoOccurrencePlusCoOccurrence {
        @Test
        fun `It should add up the counts of products present in both`() {
            // Given
            val first = CoOccurrence(mapOf(milk.productId to 1, bread.productId to 1))
            val second = CoOccurrence(mapOf(milk.productId to 2, eggs.productId to 1))

            // When
            val combined = first + second

            assertThat(combined).isEqualTo(
                CoOccurrence(mapOf(milk.productId to 3, bread.productId to 1, eggs.productId to 1)),
            )
        }
    }

    @Nested
    inner class Rollup {
        @Test
        fun `It should merge a list of co-occurrences into one`() {
            // Given
            val coOccurrences = listOf(
                CoOccurrence(mapOf(milk.productId to 1)),
                CoOccurrence(mapOf(milk.productId to 2, bread.productId to 1)),
            )

            // When
            val rolledUp = coOccurrences.rollup()

            assertThat(rolledUp).isEqualTo(CoOccurrence(mapOf(milk.productId to 3, bread.productId to 1)))
        }

        @Test
        fun `It should be empty for no co-occurrences`() {
            // Given
            val coOccurrences = emptyList<CoOccurrence>()

            // When
            val rolledUp = coOccurrences.rollup()

            assertThat(rolledUp).isEqualTo(CoOccurrence())
        }

        @Test
        fun `It should exclude the products the order already contains`() {
            // Given
            val context = CoOccurrencesWithContext(
                order = order,
                coOccurrences = listOf(
                    CoOccurrence(mapOf(milk.productId to 1, eggs.productId to 1)),
                    CoOccurrence(mapOf(bread.productId to 1, eggs.productId to 1)),
                ),
            )

            // When
            val rolledUp = context.rollup()

            assertThat(rolledUp).isEqualTo(CoOccurrence(mapOf(eggs.productId to 2)))
        }
    }

    @Nested
    inner class ToCoDistribution {
        @Test
        fun `It should express each count as a share of the total`() {
            // Given
            val coOccurrence = CoOccurrence(mapOf(milk.productId to 2, bread.productId to 1))

            // When
            val distribution = coOccurrence.toCoDistribution()

            assertThat(distribution).isEqualTo(
                CoDistribution(mapOf(milk.productId to 2.0 / 3.0, bread.productId to 1.0 / 3.0)),
            )
        }

        @Test
        fun `It should be empty when nothing co-occurred`() {
            // Given
            val coOccurrence = CoOccurrence()

            // When
            val distribution = coOccurrence.toCoDistribution()

            assertThat(distribution).isEqualTo(CoDistribution())
        }
    }

    @Nested
    inner class TopN {
        @Test
        fun `It should keep the most probable products, most probable first`() {
            // Given
            val distribution = CoDistribution(
                mapOf(milk.productId to 0.2, bread.productId to 0.5, eggs.productId to 0.3),
            )

            // When
            val top = distribution.topN(2)

            assertThat(top).isEqualTo(CoDistribution(mapOf(bread.productId to 0.5, eggs.productId to 0.3)))
        }

        @Test
        fun `It should keep everything when asked for more than it holds`() {
            // Given
            val distribution = CoDistribution(mapOf(milk.productId to 1.0))

            // When
            val top = distribution.topN(5)

            assertThat(top).isEqualTo(distribution)
        }
    }

    @Nested
    inner class WithContext {
        @Test
        fun `It should keep the co-occurrence it was given`() {
            // Given
            val coOccurrence = CoOccurrence(mapOf(milk.productId to 1))

            // When
            val context = coOccurrence.withContext(order)

            assertThat(context).isEqualTo(CoOccurrenceWithContext(coOccurrence, order))
        }

        @Test
        fun `It should substitute an empty co-occurrence when the product has none yet`() {
            // Given
            val coOccurrence: CoOccurrence? = null

            // When
            val context = coOccurrence.withContext(order)

            assertThat(context).isEqualTo(CoOccurrenceWithContext(CoOccurrence(), order))
        }
    }

    @Nested
    inner class CoOccurrencesWithContextCompleteness {
        @Test
        fun `It should be complete once every ordered product contributed`() {
            // Given
            val context = CoOccurrencesWithContext(
                order = order,
                coOccurrences = listOf(CoOccurrence(), CoOccurrence()),
            )

            // When
            val complete = context.isComplete()

            assertThat(complete).isTrue()
        }

        @Test
        fun `It should be incomplete while a product is still missing`() {
            // Given
            val context = CoOccurrencesWithContext(order = order, coOccurrences = listOf(CoOccurrence()))

            // When
            val complete = context.isComplete()

            assertThat(complete).isFalse()
        }

        @Test
        fun `It should be complete for the initial empty aggregate without an order`() {
            // Given
            val context = CoOccurrencesWithContext()

            // When
            val complete = context.isComplete()

            assertThat(complete).isTrue()
        }

        @Test
        fun `It should take the order from the co-occurrence being added`() {
            // Given
            val context = CoOccurrencesWithContext()
            val added = CoOccurrenceWithContext(CoOccurrence(mapOf(eggs.productId to 1)), order)

            // When
            val aggregate = context + added

            assertThat(aggregate).isEqualTo(
                CoOccurrencesWithContext(order = order, coOccurrences = listOf(added.coOccurrence)),
            )
        }
    }

    @Nested
    inner class OrderIdOfCoOccurrenceWithContext {
        @Test
        fun `It should report the order the context belongs to`() {
            // Given
            val context = CoOccurrenceWithContext(CoOccurrence(), order)

            // When
            val orderId = context.orderId

            assertThat(orderId).isEqualTo(order.orderId)
        }
    }

    @Nested
    inner class ProductsWithProbabilityContextCompleteness {
        @Test
        fun `It should be complete once as many products arrived as were expected`() {
            // Given
            val context = ProductsWithProbabilityContext(listOf(milk.withProbability(0.5)), 1)

            // When
            val complete = context.isComplete()

            assertThat(complete).isTrue()
        }

        @Test
        fun `It should be incomplete while fewer products arrived than expected`() {
            // Given
            val context = ProductsWithProbabilityContext(listOf(milk.withProbability(0.5)), 2)

            // When
            val complete = context.isComplete()

            assertThat(complete).isFalse()
        }

        @Test
        fun `It should take the expected size from the product being added`() {
            // Given
            val context = ProductsWithProbabilityContext()
            val added = ProductWithProbabilityContext(milk.withProbability(0.5), order.orderId, 2)

            // When
            val aggregate = context + added

            assertThat(aggregate).isEqualTo(
                ProductsWithProbabilityContext(listOf(milk.withProbability(0.5)), 2),
            )
        }
    }

    @Nested
    inner class ToProductsWithProbability {
        @Test
        fun `It should leave out the products that could not be looked up`() {
            // Given
            val context = ProductsWithProbabilityContext(listOf(milk.withProbability(0.5), null), 2)

            // When
            val products = context.toProductsWithProbability()

            assertThat(products).isEqualTo(ProductsWithProbability(listOf(milk.withProbability(0.5))))
        }
    }

    @Nested
    inner class WithProbability {
        @Test
        fun `It should pair the product with its probability`() {
            // Given
            // When
            val productWithProbability = milk.withProbability(0.7)

            assertThat(productWithProbability).isEqualTo(ProductWithProbability(milk, 0.7))
        }
    }

    @Nested
    inner class WithProduct {
        @Test
        fun `It should pair the looked-up product with the probability from the context`() {
            // Given
            val probabilityContext = ProbabilityContext(0.8, order.orderId, 2)

            // When
            val context = probabilityContext.withProduct(milk)

            assertThat(context).isEqualTo(
                ProductWithProbabilityContext(milk.withProbability(0.8), order.orderId, 2),
            )
        }

        @Test
        fun `It should carry no product when the product is unknown`() {
            // Given
            val probabilityContext = ProbabilityContext(0.8, order.orderId, 2)

            // When
            val context = probabilityContext.withProduct(null)

            assertThat(context.productWithProbability).isNull()
        }
    }
}
