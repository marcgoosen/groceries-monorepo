package io.github.marcgoosen.groceries.recommender

import assertk.assertThat
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.Test

class ExtensionsTest {
    @Test
    fun `It should pair every element with every other element, in both directions`() {
        // Given
        val products = listOf("p1", "p2", "p3")

        // When
        val pairs = products.allOrderedPairs()

        assertThat(pairs).isEqualTo(
            listOf(
                "p1" to "p2",
                "p1" to "p3",
                "p2" to "p1",
                "p2" to "p3",
                "p3" to "p1",
                "p3" to "p2",
            ),
        )
    }

    @Test
    fun `It should produce no pairs for a single element`() {
        // Given
        val products = listOf("p1")

        // When
        val pairs = products.allOrderedPairs()

        assertThat(pairs).isEqualTo(emptyList())
    }

    @Test
    fun `It should produce no pairs for no elements`() {
        // Given
        val products = emptyList<String>()

        // When
        val pairs = products.allOrderedPairs()

        assertThat(pairs).isEqualTo(emptyList())
    }
}
