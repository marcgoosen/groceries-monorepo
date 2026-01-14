package io.github.marcgoosen.groceries.recommender

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaSerde
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.github.marcgoosen.groceries.recommender.kafkastreams.AvroSerdes
import io.github.marcgoosen.groceries.recommender.kafkastreams.Topic
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopicNameBuilder
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopologyBuilder
import io.github.serpro69.kfaker.Faker
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG
import org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.AfterEach

abstract class BaseTopologyBuilderTest {
    protected val faker = Faker()
    protected val schemaRegistryUrl = "mock://dummy:1234"
    protected lateinit var topologyTestDriver: TopologyTestDriver
    protected val applicationId = "test-app"
    protected val topics = Topic.entries.associate { it.value to "${it.value}-test" }

    protected val topicNameBuilder = TopicNameBuilder(topics)

    protected val Topic.topicName get() = topicNameBuilder.build(this)

    @OptIn(ExperimentalAvro4kApi::class)
    protected val config: Config =
        Config(
            main = Config.MainConfig(
                logbackConfigFile = "",
            ),
            kafka = mapOf(
                SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
                APPLICATION_ID_CONFIG to applicationId,
                BOOTSTRAP_SERVERS_CONFIG to "dummy:1234",
                DEFAULT_KEY_SERDE_CLASS_CONFIG to ReflectAvro4kKafkaSerde::class.java.name,
                DEFAULT_VALUE_SERDE_CLASS_CONFIG to ReflectAvro4kKafkaSerde::class.java.name,
            ),
            topics = Config.Topics(
                name = topics,
            ),
        )

    protected val avroSerdes = AvroSerdes(config.kafka)

    protected fun setup(block: TopologyBuilder.() -> Unit = {}) {
        val builder = StreamsBuilder()
        val avroSerdes = AvroSerdes(config.kafka)
        val topicNameBuilder = TopicNameBuilder(config.topics.name)
        val topology = TopologyBuilder(
            builder,
            avroSerdes,
            topicNameBuilder,
        ).build(block)

        topologyTestDriver = TopologyTestDriver(
            topology,
            config.kafka.toProperties(),
        )
    }

    @AfterEach
    fun onTearDown() {
        topologyTestDriver.close()
    }
}
