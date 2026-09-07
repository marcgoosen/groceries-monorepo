package io.github.marcgoosen.groceries.recommender.kafkastreams

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaSerde
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.github.marcgoosen.groceries.recommender.Config
import io.github.serpro69.kfaker.Faker
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG
import org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.AfterEach
@OptIn(ExperimentalAvro4kApi::class)
abstract class BaseTopologyTest {
    protected val faker = Faker()
    protected lateinit var topologyTestDriver: TopologyTestDriver

    private val topics = Topic.entries.associate { it.value to "${it.value}-test" }

    protected val topicNameBuilder = TopicNameBuilder(topics)

    protected val Topic.topicName get() = topicNameBuilder.build(this)

    protected val config = Config(
        main = Config.MainConfig(logbackConfigFile = ""),
        kafka = mapOf(
            SCHEMA_REGISTRY_URL_CONFIG to "mock://dummy:1234",
            APPLICATION_ID_CONFIG to "test-app",
            BOOTSTRAP_SERVERS_CONFIG to "dummy:1234",
            DEFAULT_KEY_SERDE_CLASS_CONFIG to ReflectAvro4kKafkaSerde::class.java.name,
            DEFAULT_VALUE_SERDE_CLASS_CONFIG to ReflectAvro4kKafkaSerde::class.java.name,
        ),
        topics = Config.Topics(name = topics),
    )

    protected val avroSerdes = AvroSerdes(config.kafka)

    protected fun setup(block: TopologyBuilder.() -> Unit = {}) {
        val topology = TopologyBuilder(
            StreamsBuilder(),
            avroSerdes,
            TopicNameBuilder(config.topics.name),
        ).build(block)

        topologyTestDriver = TopologyTestDriver(topology, config.kafka.toProperties())
    }

    @AfterEach
    fun onTearDown() {
        topologyTestDriver.close()
    }
}
