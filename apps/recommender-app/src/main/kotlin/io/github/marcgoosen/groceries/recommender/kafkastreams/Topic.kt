package io.github.marcgoosen.groceries.recommender.kafkastreams

enum class Topic(val value: String) {
    ORDER("orders"),
    PRODUCT("products"),
    RELATED_PRODUCTS("related-products"),
    ;

    companion object {
        fun parse(topic: String?) = entries.firstOrNull { it.value == topic }
    }
}
