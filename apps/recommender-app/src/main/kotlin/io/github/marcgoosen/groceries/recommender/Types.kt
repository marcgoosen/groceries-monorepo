package io.github.marcgoosen.groceries.recommender

import io.github.marcgoosen.groceries.recommender.domain.CoDistribution
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrence
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrenceWithContext
import io.github.marcgoosen.groceries.recommender.domain.CoOccurrencesWithContext
import io.github.marcgoosen.groceries.recommender.domain.ProbabilityContext
import io.github.marcgoosen.groceries.recommender.domain.ProductWithProbabilityContext
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbability
import io.github.marcgoosen.groceries.recommender.domain.ProductsWithProbabilityContext
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable

typealias ProductTable = KTable<ProductId, Product>
typealias CoOccurrenceTable = KTable<ProductId, CoOccurrence>
typealias OrderStream = KStream<OrderId, Order>
typealias OrderByProductIdStream = KStream<ProductId, Order>
typealias CoOccurrenceWithContextByProductIdStream = KStream<ProductId, CoOccurrenceWithContext>
typealias CoOccurrenceStream = KStream<OrderId, CoOccurrence>
typealias ProbabilityContextStream = KStream<ProductId, ProbabilityContext>
typealias ProductsWithProbabilityStream = KStream<OrderId, ProductsWithProbability>
typealias CoDistributionStream = KStream<OrderId, CoDistribution>
typealias ProductWithProbabilityContextStream = KStream<ProductId, ProductWithProbabilityContext>
typealias ProductsWithProbabilityContextStream = KStream<OrderId, ProductsWithProbabilityContext>
typealias CoOccurrenceWithContextStream = KStream<OrderId, CoOccurrenceWithContext>
typealias CoOccurrencesWithContextStream = KStream<OrderId, CoOccurrencesWithContext>
typealias ProductIdStream = KStream<ProductId, ProductId>
