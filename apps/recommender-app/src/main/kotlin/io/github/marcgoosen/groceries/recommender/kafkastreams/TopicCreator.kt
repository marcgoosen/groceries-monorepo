package io.github.marcgoosen.groceries.recommender.kafkastreams

import io.github.marcgoosen.groceries.recommender.Config
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.NewTopic
import java.util.*

private val logger = KotlinLogging.logger {}

class TopicCreator(private val admin: Admin, private val topicNameBuilder: TopicNameBuilder) {
    fun createTopics(create: Map<String, Config.TopicConfig>) {
        admin.createTopics(
            create.map { (topicName, topicConfig) ->
                NewTopic(
                    topicNameBuilder.build(
                        Topic.parse(topicName) ?: throw IllegalArgumentException("Unknown topic: $topicName"),
                    ),
                    Optional.ofNullable(topicConfig.partitions),
                    Optional.ofNullable(topicConfig.replicationFactor),
                )
                    .configs(topicConfig.configs)
            },
        ).values().forEach { (topic, future) ->
            runCatching { future.get() }
                .onFailure { logger.debug { "Topic $topic already exists" } }
                .onSuccess { logger.info { "Created topic $topic" } }
        }
    }
}
