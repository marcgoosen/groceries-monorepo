package io.github.marcgoosen.groceries.recommender

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaSerde
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.github.marcgoosen.groceries.recommender.domain.RelatedProducts
import io.github.marcgoosen.groceries.recommender.kafkastreams.AvroSerdes
import io.github.marcgoosen.groceries.recommender.kafkastreams.Topic
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopicCreator
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopicNameBuilder
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopologyBuilder
import io.github.marcgoosen.groceries.shared.domain.Order
import io.github.marcgoosen.groceries.shared.domain.OrderId
import io.github.marcgoosen.groceries.shared.domain.OrderLine
import io.github.marcgoosen.groceries.shared.domain.Product
import io.github.marcgoosen.groceries.shared.domain.ProductId
import kotlinx.datetime.Clock
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import java.net.URI
import java.time.Duration
import kotlin.io.path.createTempDirectory

private const val KAFKA_ALIAS = "kafka"
private const val KAFKA_INTERNAL_PORT = 19092
private const val SCHEMA_REGISTRY_PORT = 8081

/**
 * The unit tests drive the topology through TopologyTestDriver against a mock Schema Registry, which never exercises
 * schema registration, repartition topics or the real serde path. This runs the same topology on a real broker with
 * a real Schema Registry, which is where those failures actually live.
 */
@OptIn(ExperimentalAvro4kApi::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecommendationPipelineIntegrationTest {
    private val network: Network = Network.newNetwork()

    private val kafka = KafkaContainer("apache/kafka-native:4.3.1")
        .withNetwork(network)
        .withNetworkAliases(KAFKA_ALIAS)
        .withListener("$KAFKA_ALIAS:$KAFKA_INTERNAL_PORT")

    private val schemaRegistry = GenericContainer("confluentinc/cp-schema-registry:8.3.1")
        .withNetwork(network)
        .withExposedPorts(SCHEMA_REGISTRY_PORT)
        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:$SCHEMA_REGISTRY_PORT")
        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://$KAFKA_ALIAS:$KAFKA_INTERNAL_PORT")
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))

    private lateinit var config: Config
    private lateinit var avroSerdes: AvroSerdes
    private lateinit var topicNameBuilder: TopicNameBuilder
    private lateinit var streams: KafkaStreams

    private val milk = Product("p1", "Milk", 2.0)
    private val bread = Product("p2", "Bread", 1.5)
    private val eggs = Product("p3", "Eggs", 3.0)

    @BeforeAll
    fun startPipeline() {
        kafka.start()
        schemaRegistry.start()

        config = configFor(
            kafka.bootstrapServers,
            "http://${schemaRegistry.host}:${schemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT)}",
        )
        avroSerdes = AvroSerdes(config.kafka)
        topicNameBuilder = TopicNameBuilder(config.topics.name)

        Admin.create(config.kafka.toProperties()).use {
            TopicCreator(it, topicNameBuilder).createTopics(config.topics.create)
        }

        publishCatalogue()

        streams = KafkaStreams(
            TopologyBuilder(StreamsBuilder(), topicNameBuilder).build(),
            config.kafka.toProperties(),
        )
        streams.start()
        awaitRunning()
    }

    @AfterAll
    fun stopPipeline() {
        if (::streams.isInitialized) streams.close(Duration.ofSeconds(30))
        schemaRegistry.stop()
        kafka.stop()
        network.close()
    }

    @Test
    fun `It should recommend the products bought alongside the ordered one`() {
        // Given
        produce(order("training-1", milk, bread, eggs))

        // When
        produce(order("target-1", milk))

        assertThat(awaitRecommendation("target-1").relatedProducts.map { it.product }.toSet())
            .isEqualTo(setOf(bread, eggs))
    }

    @Test
    fun `It should register a schema for every topic it writes`() {
        // Given
        produce(order("training-2", milk, bread))
        produce(order("target-2", milk))
        awaitRecommendation("target-2")

        // When
        val subjects = schemaRegistrySubjects()

        assertThat(subjects.contains("${topicNameBuilder.build(Topic.RELATED_PRODUCTS)}-value")).isEqualTo(true)
    }

    private fun configFor(bootstrapServers: String, schemaRegistryUrl: String) = Config(
        main = Config.MainConfig(logbackConfigFile = "logback-test.xml"),
        kafka = mapOf(
            "application.id" to "groceries-recommender-integration",
            "bootstrap.servers" to bootstrapServers,
            SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
            "default.key.serde" to ReflectAvro4kKafkaSerde::class.java.name,
            "default.value.serde" to ReflectAvro4kKafkaSerde::class.java.name,
            "auto.offset.reset" to "earliest",
            "auto.register.schemas" to "true",
            "commit.interval.ms" to "100",
            "state.dir" to createTempDirectory("kafka-streams-integration").toString(),
            "topology.optimization" to "all",
            "ensure.explicit.internal.resource.naming" to "true",
        ),
        topics = Config.Topics(
            name = Topic.entries.associate { it.value to "integration.${it.value}.v1" },
            create = Topic.entries.associate {
                it.value to Config.TopicConfig(partitions = 1, replicationFactor = 1)
            },
        ),
    )

    private fun publishCatalogue() {
        KafkaProducer<ProductId, Product>(
            config.kafka.toProperties(),
            avroSerdes.string.serializer(),
            avroSerdes.create<Product>().serializer(),
        ).use { producer ->
            listOf(milk, bread, eggs).forEach {
                producer.send(ProducerRecord(topicNameBuilder.build(Topic.PRODUCT), it.productId, it))
            }
            producer.flush()
        }
    }

    private fun order(orderId: OrderId, vararg products: Product) = Order(
        orderId = orderId,
        orderLines = products.map { OrderLine(it.productId, it.price, 1) },
        timestamp = Clock.System.now(),
    )

    private fun produce(order: Order) {
        KafkaProducer<OrderId, Order>(
            config.kafka.toProperties(),
            avroSerdes.string.serializer(),
            avroSerdes.create<Order>().serializer(),
        ).use {
            it.send(ProducerRecord(topicNameBuilder.build(Topic.ORDER), order.orderId, order)).get()
        }
    }

    private fun awaitRunning() {
        val deadline = System.currentTimeMillis() + 60_000
        while (streams.state() != KafkaStreams.State.RUNNING && System.currentTimeMillis() < deadline) {
            Thread.sleep(200)
        }
        check(streams.state() == KafkaStreams.State.RUNNING) { "Streams did not start: ${streams.state()}" }
    }

    private fun awaitRecommendation(orderId: OrderId): RelatedProducts {
        val properties = config.kafka.toProperties().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "integration-$orderId")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }

        KafkaConsumer<OrderId, RelatedProducts>(
            properties,
            avroSerdes.string.deserializer(),
            avroSerdes.create<RelatedProducts>().deserializer(),
        ).use { consumer ->
            consumer.subscribe(listOf(topicNameBuilder.build(Topic.RELATED_PRODUCTS)))
            val deadline = System.currentTimeMillis() + 60_000
            while (System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500))
                    .firstOrNull { it.key() == orderId }
                    ?.let { return it.value() }
            }
        }
        error("No recommendation was produced for $orderId")
    }

    private fun schemaRegistrySubjects(): String {
        val url = "http://${schemaRegistry.host}:${schemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT)}/subjects"
        return URI(url).toURL().readText()
    }
}
