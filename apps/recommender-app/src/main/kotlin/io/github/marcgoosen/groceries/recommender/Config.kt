package io.github.marcgoosen.groceries.recommender

import kotlinx.serialization.Serializable

@Serializable
data class Config(val main: MainConfig, val kafka: Map<String, String>, val topics: Topics) {
    @Serializable
    data class MainConfig(
        val applicationId: String = "groceries-recommender",
        val port: Int = 8080,
        val logbackConfigFile: String,
        val createTopics: Boolean = false,
        val startSimulator: Boolean = false,
    )

    @Serializable
    data class Topics(val name: Map<String, String>, val create: Map<String, TopicConfig> = emptyMap())

    @Serializable
    data class TopicConfig(
        val partitions: Int? = null,
        val replicationFactor: Short? = null,
        val configs: Map<String, String>? = null,
    )
}
