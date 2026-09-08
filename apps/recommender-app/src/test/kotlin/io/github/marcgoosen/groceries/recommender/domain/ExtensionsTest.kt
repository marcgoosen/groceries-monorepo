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
            val alsoBought = AlsoBoughtCount(mapOf(milk.productId to 2, bread.productId to 3))

            // When
            val totalCount = alsoBought.totalCount

            assertThat(totalCount).isEqualTo(5)
        }

        @Test
        fun `It should be zero when nothing co-occurred`() {
            // Given
            val alsoBought = AlsoBoughtCount()

            // When
            val totalCount = alsoBought.totalCount

            assertThat(totalCount).isEqualTo(0)
        }
    }

    @Nested
    inner class AlsoBoughtCountPlusProductId {
        @Test
        fun `It should start a count for a product seen for the first time`() {
            // Given
            val alsoBought = AlsoBoughtCount()

            // When
            val incremented = alsoBought + milk.productId

            assertThat(incremented).isEqualTo(AlsoBoughtCount(mapOf(milk.productId to 1)))
        }

        @Test
        fun `It should raise the count of a product seen before`() {
            // Given
            val alsoBought = AlsoBoughtCount(mapOf(milk.productId to 2))

            // When
            val incremented = alsoBought + milk.productId

            assertThat(incremented).isEqualTo(AlsoBoughtCount(mapOf(milk.productId to 3)))
        }
    }

    @Nested
    inner class AlsoBoughtCountMinus {
        @Test
        fun `It should drop a single product`() {
            // Given
            val alsoBought = AlsoBoughtCount(mapOf(milk.productId to 2, bread.productId to 1))

            // When
            val remaining = alsoBought - milk.productId

            assertThat(remaining).isEqualTo(AlsoBoughtCount(mapOf(bread.productId to 1)))
        }

        @Test
        fun `It should drop every product in the given set`() {
            // Given
            val alsoBought = AlsoBoughtCount(
                mapOf(
                    milk.productId to 2,
                    bread.productId to 1,
                    eggs.productId to 4,
                ),
            )

            // When
            val remaining = alsoBought - setOf(milk.productId, bread.productId)

            assertThat(remaining).isEqualTo(AlsoBoughtCount(mapOf(eggs.productId to 4)))
        }
    }

    @Nested
    inner class AlsoBoughtCountPlusAlsoBoughtCount {
        @Test
        fun `It should add up the counts of products present in both`() {
            // Given
            val first = AlsoBoughtCount(mapOf(milk.productId to 1, bread.productId to 1))
            val second = AlsoBoughtCount(mapOf(milk.productId to 2, eggs.productId to 1))

            // When
            val combined = first + second

            assertThat(combined).isEqualTo(
                AlsoBoughtCount(mapOf(milk.productId to 3, bread.productId to 1, eggs.productId to 1)),
            )
        }
    }

    @Nested
    inner class Sum {
        @Test
        fun `It should merge a list of also-bought into one`() {
            // Given
            val alsoBought = listOf(
                AlsoBoughtCount(mapOf(milk.productId to 1)),
                AlsoBoughtCount(mapOf(milk.productId to 2, bread.productId to 1)),
            )

            // When
            val rolledUp = alsoBought.sum()

            assertThat(rolledUp).isEqualTo(AlsoBoughtCount(mapOf(milk.productId to 3, bread.productId to 1)))
        }

        @Test
        fun `It should be empty for no also-bought`() {
            // Given
            val alsoBought = emptyList<AlsoBoughtCount>()

            // When
            val rolledUp = alsoBought.sum()

            assertThat(rolledUp).isEqualTo(AlsoBoughtCount())
        }

        @Test
        fun `It should exclude the products the order already contains`() {
            // Given
            val context = AlsoBoughtSoFar(
                order = order,
                alsoBought = listOf(
                    AlsoBoughtCount(mapOf(milk.productId to 1, eggs.productId to 1)),
                    AlsoBoughtCount(mapOf(bread.productId to 1, eggs.productId to 1)),
                ),
            )

            // When
            val rolledUp = context.sum()

            assertThat(rolledUp).isEqualTo(AlsoBoughtCount(mapOf(eggs.productId to 2)))
        }
    }

    @Nested
    inner class ToAlsoBoughtProbabilities {
        @Test
        fun `It should express each count as a share of the total`() {
            // Given
            val alsoBought = AlsoBoughtCount(mapOf(milk.productId to 2, bread.productId to 1))

            // When
            val distribution = alsoBought.toAlsoBoughtProbabilities()

            assertThat(distribution).isEqualTo(
                AlsoBoughtProbabilities(mapOf(milk.productId to 2.0 / 3.0, bread.productId to 1.0 / 3.0)),
            )
        }

        @Test
        fun `It should be empty when nothing co-occurred`() {
            // Given
            val alsoBought = AlsoBoughtCount()

            // When
            val distribution = alsoBought.toAlsoBoughtProbabilities()

            assertThat(distribution).isEqualTo(AlsoBoughtProbabilities())
        }
    }

    @Nested
    inner class TopN {
        @Test
        fun `It should keep the most probable products, most probable first`() {
            // Given
            val distribution = AlsoBoughtProbabilities(
                mapOf(milk.productId to 0.2, bread.productId to 0.5, eggs.productId to 0.3),
            )

            // When
            val top = distribution.topN(2)

            assertThat(top).isEqualTo(
                AlsoBoughtProbabilities(
                    mapOf(
                        bread.productId to 0.5,
                        eggs.productId to 0.3,
                    ),
                ),
            )
        }

        @Test
        fun `It should keep everything when asked for more than it holds`() {
            // Given
            val distribution = AlsoBoughtProbabilities(mapOf(milk.productId to 1.0))

            // When
            val top = distribution.topN(5)

            assertThat(top).isEqualTo(distribution)
        }
    }

    @Nested
    inner class WithOrder {
        @Test
        fun `It should keep the also-bought it was given`() {
            // Given
            val alsoBought = AlsoBoughtCount(mapOf(milk.productId to 1))

            // When
            val context = alsoBought.withOrder(order)

            assertThat(context).isEqualTo(AlsoBoughtCountWithOrder(alsoBought, order))
        }

        @Test
        fun `It should substitute an empty also-bought when the product has none yet`() {
            // Given
            val alsoBought: AlsoBoughtCount? = null

            // When
            val context = alsoBought.withOrder(order)

            assertThat(context).isEqualTo(AlsoBoughtCountWithOrder(AlsoBoughtCount(), order))
        }
    }

    @Nested
    inner class AlsoBoughtSoFarCompleteness {
        @Test
        fun `It should be complete once every ordered product contributed`() {
            // Given
            val context = AlsoBoughtSoFar(
                order = order,
                alsoBought = listOf(AlsoBoughtCount(), AlsoBoughtCount()),
            )

            // When
            val complete = context.isComplete()

            assertThat(complete).isTrue()
        }

        @Test
        fun `It should be incomplete while a product is still missing`() {
            // Given
            val context = AlsoBoughtSoFar(order = order, alsoBought = listOf(AlsoBoughtCount()))

            // When
            val complete = context.isComplete()

            assertThat(complete).isFalse()
        }

        @Test
        fun `It should be complete for the initial empty aggregate without an order`() {
            // Given
            val context = AlsoBoughtSoFar()

            // When
            val complete = context.isComplete()

            assertThat(complete).isTrue()
        }

        @Test
        fun `It should take the order from the also-bought being added`() {
            // Given
            val context = AlsoBoughtSoFar()
            val added = AlsoBoughtCountWithOrder(AlsoBoughtCount(mapOf(eggs.productId to 1)), order)

            // When
            val aggregate = context + added

            assertThat(aggregate).isEqualTo(
                AlsoBoughtSoFar(order = order, alsoBought = listOf(added.alsoBought)),
            )
        }
    }

    @Nested
    inner class OrderIdOfAlsoBoughtCountWithOrder {
        @Test
        fun `It should report the order the context belongs to`() {
            // Given
            val context = AlsoBoughtCountWithOrder(AlsoBoughtCount(), order)

            // When
            val orderId = context.orderId

            assertThat(orderId).isEqualTo(order.orderId)
        }
    }

    @Nested
    inner class RelatedProductsSoFarCompleteness {
        @Test
        fun `It should be complete once as many products arrived as were expected`() {
            // Given
            val context = RelatedProductsSoFar(listOf(milk.withProbability(0.5)), 1)

            // When
            val complete = context.isComplete()

            assertThat(complete).isTrue()
        }

        @Test
        fun `It should be incomplete while fewer products arrived than expected`() {
            // Given
            val context = RelatedProductsSoFar(listOf(milk.withProbability(0.5)), 2)

            // When
            val complete = context.isComplete()

            assertThat(complete).isFalse()
        }

        @Test
        fun `It should take the expected size from the product being added`() {
            // Given
            val context = RelatedProductsSoFar()
            val added = RelatedProductWithOrder(milk.withProbability(0.5), order.orderId, 2)

            // When
            val aggregate = context + added

            assertThat(aggregate).isEqualTo(
                RelatedProductsSoFar(listOf(milk.withProbability(0.5)), 2),
            )
        }
    }

    @Nested
    inner class ToRelatedProducts {
        @Test
        fun `It should leave out the products that could not be looked up`() {
            // Given
            val context = RelatedProductsSoFar(listOf(milk.withProbability(0.5), null), 2)

            // When
            val products = context.toRelatedProducts()

            assertThat(products).isEqualTo(RelatedProducts(listOf(milk.withProbability(0.5))))
        }
    }

    @Nested
    inner class WithProbability {
        @Test
        fun `It should pair the product with its probability`() {
            // Given
            // When
            val relatedProduct = milk.withProbability(0.7)

            assertThat(relatedProduct).isEqualTo(RelatedProduct(milk, 0.7))
        }
    }

    @Nested
    inner class WithProduct {
        @Test
        fun `It should pair the looked-up product with the probability from the context`() {
            // Given
            val probabilityWithOrder = ProbabilityWithOrder(0.8, order.orderId, 2)

            // When
            val context = probabilityWithOrder.withProduct(milk)

            assertThat(context).isEqualTo(
                RelatedProductWithOrder(milk.withProbability(0.8), order.orderId, 2),
            )
        }

        @Test
        fun `It should carry no product when the product is unknown`() {
            // Given
            val probabilityWithOrder = ProbabilityWithOrder(0.8, order.orderId, 2)

            // When
            val context = probabilityWithOrder.withProduct(null)

            assertThat(context.relatedProduct).isNull()
        }
    }
}
