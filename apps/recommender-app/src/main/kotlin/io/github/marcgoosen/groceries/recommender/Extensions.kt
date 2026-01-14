package io.github.marcgoosen.groceries.recommender

fun <T> List<T>.allOrderedPairs(): List<Pair<T, T>> = flatMap { a ->
    filter { b -> b != a }.map { b ->
        a to b
    }
}
