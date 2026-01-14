package io.github.marcgoosen.groceries.recommender

import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.serpro69.kfaker.Faker
import kotlinx.datetime.Clock

fun <T> Faker.randomList(maxSize: Int = 5, fn: () -> T) = (1..random.nextInt(1, maxSize)).map { fn() }

fun Faker.orderId() = random.nextUUID()
fun Faker.productId() = random.nextUUID()
fun Faker.orderLine() = OrderLine(
    productId = random.nextUUID(),
    price = random.nextDouble(),
    quantity = random.nextInt(min = 1, max = 5),
)

fun Faker.order() = Order(
    orderId = orderId(),
    timestamp = Clock.System.now(),
    orderLines = randomList { productId() }.distinct().map { orderLine().copy(productId = it) },
)

fun Faker.product() = Product(
    productId = productId(),
    name = this.commerce.toString(),
    price = random.nextDouble(),
)
