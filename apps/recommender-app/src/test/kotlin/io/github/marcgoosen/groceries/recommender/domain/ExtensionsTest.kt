package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import io.github.marcgoosen.groceries.shared.domain.Product
import io.kotest.matchers.shouldBe
import kotlinx.datetime.Clock
import org.junit.jupiter.api.Test

class ExtensionsTest {

    private val p1 = Product("p1", "Product 1", 10.0)
    private val p2 = Product("p2", "Product 2", 20.0)
    private val p3 = Product("p3", "Product 3", 30.0)

    private val order =
        Order(
            orderId = "o1",
            timestamp = Clock.System.now(),
            orderLines =
            listOf(
                OrderLine(p1.productId, p1.price, 1),
                OrderLine(p2.productId, p2.price, 1),
            ),
        )

    @Test
    fun `It should compute totalCount and increment with productId`() {
        val co = CoOccurrence(mapOf(p1.productId to 2))
        co.totalCount shouldBe 2

        val c2 = co + p2.productId
        c2.countsByProduct[p2.productId] shouldBe 1
        c2.totalCount shouldBe 3

        val c3 = c2 + p1.productId
        c3.countsByProduct[p1.productId] shouldBe 3
        c3.totalCount shouldBe 4
    }

    @Test
    fun `It should subtract productId and productIds set`() {
        val co = CoOccurrence(mapOf(p1.productId to 2, p2.productId to 1))
        val withoutP1 = co - p1.productId
        withoutP1.countsByProduct.containsKey(p1.productId) shouldBe false

        val withoutSet = co - setOf(p2.productId)
        withoutSet.countsByProduct.containsKey(p2.productId) shouldBe false
    }

    @Test
    fun `It should combine co-occurrences by plus and rollup a list`() {
        val a = CoOccurrence(mapOf(p1.productId to 1, p2.productId to 1))
        val b = CoOccurrence(mapOf(p1.productId to 2, p3.productId to 1))
        val combined = a + b
        combined.countsByProduct[p1.productId] shouldBe 3
        combined.countsByProduct[p2.productId] shouldBe 1
        combined.countsByProduct[p3.productId] shouldBe 1

        val listRollup = listOf(a, b).rollup()
        listRollup shouldBe combined
    }

    @Test
    fun `It should convert to CoDistribution and choose topN`() {
        val co = CoOccurrence(mapOf(p1.productId to 2, p2.productId to 1))
        val dist = co.toCoDistribution()
        dist.probabilityByProductId[p1.productId] shouldBe (2.0 / 3.0)
        dist.probabilityByProductId[p2.productId] shouldBe (1.0 / 3.0)

        val top1 = dist.topN(1)
        top1.probabilityByProductId.size shouldBe 1
        top1.probabilityByProductId.keys.first() shouldBe p1.productId
    }

    @Test
    fun `It should convert any empty CoDistribution and choose topN`() {
        val co = CoOccurrence()
        val dist = co.toCoDistribution()

        dist shouldBe CoDistribution()
    }

    @Test
    fun `It should get productIds from Order`() {
        order.productIds shouldBe setOf(p1.productId, p2.productId)
    }

    @Test
    fun `It should check if CoOccurrencesWithContext is complete`() {
        val coc =
            CoOccurrencesWithContext(
                order = order,
                coOccurrences = listOf(CoOccurrence(), CoOccurrence()),
            )
        coc.isComplete() shouldBe true

        val incomplete = coc.copy(coOccurrences = listOf(CoOccurrence()))
        incomplete.isComplete() shouldBe false
    }

    @Test
    fun `It should add CoOccurrence to CoOccurrencesWithContext`() {
        val coc1 =
            CoOccurrencesWithContext(
                order = order,
                coOccurrences = listOf(CoOccurrence(mapOf(p1.productId to 1))),
            )
        val singular = CoOccurrenceWithContext(CoOccurrence(mapOf(p2.productId to 1)), order)

        val result = coc1 + singular
        result.order shouldBe order
        result.coOccurrences.size shouldBe 2
        result.coOccurrences[1].countsByProduct[p2.productId] shouldBe 1
    }

    @Test
    fun `It should rollup CoOccurrencesWithContext and remove order productIds`() {
        val a = CoOccurrence(mapOf(p1.productId to 1, p3.productId to 1))
        val b = CoOccurrence(mapOf(p2.productId to 1, p3.productId to 1))
        val coc = CoOccurrencesWithContext(order = order, coOccurrences = listOf(a, b))

        // order has p1 and p2
        // rollup of a+b has p1, p2, p3
        // after rollup() and subtract order productIds, only p3 should remain
        val result = coc.rollup()
        result.countsByProduct.keys shouldBe setOf(p3.productId)
        result.countsByProduct[p3.productId] shouldBe 2
    }

    @Test
    fun `It should create CoOccurrenceWithContext from nullable CoOccurrence`() {
        val co: CoOccurrence? = null
        val coc = co.withContext(order)
        coc.coOccurrence shouldBe CoOccurrence()
        coc.order shouldBe order

        val nonNullCo = CoOccurrence(mapOf(p1.productId to 1))
        val coc2 = nonNullCo.withContext(order)
        coc2.coOccurrence shouldBe nonNullCo
    }

    @Test
    fun `It should get orderId from CoOccurrenceWithContext`() {
        val coc = CoOccurrenceWithContext(CoOccurrence(), order)
        coc.orderId shouldBe order.orderId
    }

    @Test
    fun `It should check if ProductsWithProbabilityContext is complete`() {
        val pwpc =
            ProductsWithProbabilityContext(
                productsWithProbabilities = listOf(p1.withProbability(0.5)),
                expectedSize = 1,
            )
        pwpc.isComplete() shouldBe true

        val pwpc2 = pwpc.copy(expectedSize = 2)
        pwpc2.isComplete() shouldBe false
    }

    @Test
    fun `It should convert ProductsWithProbabilityContext to ProductsWithProbability`() {
        val pwp = p1.withProbability(0.5)
        val pwpc =
            ProductsWithProbabilityContext(
                productsWithProbabilities = listOf(pwp, null),
                expectedSize = 2,
            )
        val result = pwpc.toProductsWithProbability()
        result.productsWithProbabilities shouldBe listOf(pwp)
    }

    @Test
    fun `It should add ProductWithProbabilityContext to ProductsWithProbabilityContext`() {
        val pwp1 = p1.withProbability(0.5)
        val pwp2 = p2.withProbability(0.3)
        val context = ProductsWithProbabilityContext(listOf(pwp1), 2)
        val singular = ProductWithProbabilityContext(pwp2, "o1", 2)

        val result = context + singular
        result.expectedSize shouldBe 2
        result.productsWithProbabilities shouldBe listOf(pwp1, pwp2)
    }

    @Test
    fun `It should create ProductWithProbability from Product`() {
        val pwp = p1.withProbability(0.7)
        pwp.product shouldBe p1
        pwp.probability shouldBe 0.7
    }

    @Test
    fun `It should create ProductWithProbabilityContext from ProbabilityContext`() {
        val probCtx = ProbabilityContext(0.8, "o1", 2)
        val result = probCtx.withProduct(p1)

        result.orderId shouldBe "o1"
        result.expectedSize shouldBe 2
        result.productWithProbability?.product shouldBe p1
        result.productWithProbability?.probability shouldBe 0.8

        val nullResult = probCtx.withProduct(null)
        nullResult.productWithProbability shouldBe null
    }
}
