package io.github.marcgoosen.groceries.recommender.kafkastreams

class TopicNameBuilder(private val topics: Map<String, String>) {
    fun build(topic: Topic) = topics[topic.value] ?: throw IllegalArgumentException("Topic ${topic.value} not found")
}
