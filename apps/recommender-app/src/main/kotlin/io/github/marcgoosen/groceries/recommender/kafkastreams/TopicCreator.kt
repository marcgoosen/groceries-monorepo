package io.github.marcgoosen.groceries.recommender.kafkastreams

import io.github.marcgoosen.groceries.recommender.Config
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.TopicExistsException
import java.util.Optional

private val logger = KotlinLogging.logger {}

class TopicCreator(private val admin: Admin, private val topicNameBuilder: TopicNameBuilder) {
    fun createTopics(create: Map<String, Config.TopicConfig>) {
        val failed = admin.createTopics(create.map { (name, topicConfig) -> newTopic(name, topicConfig) })
            .values()
            .filterNot { (topic, future) -> isCreated(topic, future) }
            .keys

        check(failed.isEmpty()) { "Could not create topics: $failed" }
    }

    private fun newTopic(name: String, topicConfig: Config.TopicConfig) = NewTopic(
        topicNameBuilder.build(Topic.parse(name) ?: throw IllegalArgumentException("Unknown topic: $name")),
        Optional.ofNullable(topicConfig.partitions),
        Optional.ofNullable(topicConfig.replicationFactor),
    )
        .configs(topicConfig.configs)

    private fun isCreated(topic: String, future: KafkaFuture<Void>): Boolean {
        val failure = runCatching { future.get() }.exceptionOrNull()
        return when {
            failure == null -> {
                logger.info { "Created topic $topic" }
                true
            }

            failure.cause is TopicExistsException -> {
                logger.debug { "Topic $topic already exists" }
                true
            }

            else -> {
                logger.warn(failure) { "Failed to create topic $topic" }
                false
            }
        }
    }
}
