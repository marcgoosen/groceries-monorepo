package io.github.marcgoosen.groceries.recommender

import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCount
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtCountWithOrder
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtProbabilities
import io.github.marcgoosen.groceries.recommender.domain.AlsoBoughtSoFar
import io.github.marcgoosen.groceries.recommender.domain.ProbabilityWithOrder
import io.github.marcgoosen.groceries.recommender.domain.RelatedProductWithOrder
import io.github.marcgoosen.groceries.recommender.domain.RelatedProducts
import io.github.marcgoosen.groceries.recommender.domain.RelatedProductsSoFar
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable

typealias ProductTable = KTable<ProductId, Product>
typealias AlsoBoughtTable = KTable<ProductId, AlsoBoughtCount>
typealias OrderStream = KStream<OrderId, Order>
typealias OrderByProductIdStream = KStream<ProductId, Order>
typealias AlsoBoughtCountWithOrderByProductIdStream = KStream<ProductId, AlsoBoughtCountWithOrder>
typealias AlsoBoughtStream = KStream<OrderId, AlsoBoughtCount>
typealias ProbabilityWithOrderStream = KStream<ProductId, ProbabilityWithOrder>
typealias RelatedProductsStream = KStream<OrderId, RelatedProducts>
typealias AlsoBoughtProbabilitiesStream = KStream<OrderId, AlsoBoughtProbabilities>
typealias RelatedProductWithOrderStream = KStream<ProductId, RelatedProductWithOrder>
typealias RelatedProductsSoFarStream = KStream<OrderId, RelatedProductsSoFar>
typealias AlsoBoughtCountWithOrderStream = KStream<OrderId, AlsoBoughtCountWithOrder>
typealias AlsoBoughtSoFarStream = KStream<OrderId, AlsoBoughtSoFar>
typealias ProductIdStream = KStream<ProductId, ProductId>
