package io.github.marcgoosen.groceries.recommender.domain

import io.github.marcgoosen.groceries.shared.domain.Order
import kotlinx.serialization.Serializable

@Serializable
data class CoOccurrencesWithContext(val coOccurrences: List<CoOccurrence> = emptyList(), val order: Order? = null)
