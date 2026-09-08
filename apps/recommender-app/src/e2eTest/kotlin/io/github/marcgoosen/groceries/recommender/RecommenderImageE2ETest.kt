package io.github.marcgoosen.groceries.recommender

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import assertk.assertions.isNotEmpty
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.github.marcgoosen.groceries.recommender.domain.RelatedProducts
import io.github.marcgoosen.groceries.recommender.kafkastreams.AvroSerdes
import io.github.marcgoosen.groceries.recommender.kafkastreams.Topic
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopicNameBuilder
import io.github.marcgoosen.groceries.shared.domain.OrderId
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID

private const val READY_TIMEOUT_SECONDS = 120L
private const val RECOMMENDATION_TIMEOUT_SECONDS = 120L

/**
 * Asserts against the packaged image running in the compose stack, which is the only test that covers the
 * container itself: its entrypoint, its non-root user and the in-network addresses it is configured with.
 * Start the stack first with `docker compose --profile app up -d`.
 *
 * It produces nothing of its own — the container runs the simulator, so this only has to observe.
 */
@OptIn(ExperimentalAvro4kApi::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecommenderImageE2ETest {
    private val recommenderUrl = env("RECOMMENDER_URL", "http://localhost:8080")
    private val bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    private val schemaRegistryUrl = env("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081")

    private val http = HttpClient.newHttpClient()

    private val topicNameBuilder = TopicNameBuilder(
        mapOf(
            Topic.PRODUCT.value to "groceries.products.v1",
            Topic.ORDER.value to "groceries.orders.v1",
            Topic.RELATED_PRODUCTS.value to "groceries.related-products.v1",
        ),
    )

    private val avroSerdes = AvroSerdes(mapOf(SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl))

    @BeforeAll
    fun awaitReady() {
        val deadline = System.currentTimeMillis() + READY_TIMEOUT_SECONDS * 1000
        while (System.currentTimeMillis() < deadline) {
            if (runCatching { get("/health/readiness").statusCode() }.getOrNull() == 200) return
            Thread.sleep(1_000)
        }
        error("The container never became ready at $recommenderUrl within $READY_TIMEOUT_SECONDS s")
    }

    @Test
    fun `It should report itself ready`() {
        // Given
        // When
        val response = get("/health/readiness")

        assertThat(response.statusCode()).isEqualTo(200)
        assertThat(response.body()).contains("\"status\":\"UP\"")
    }

    @Test
    fun `It should publish the stream state it was supervised with`() {
        // Given
        // When
        val scrape = get("/prometheus").body()

        assertThat(scrape).contains("kafka_streams_state")
    }

    @Test
    fun `It should produce recommendations from the orders it generates itself`() {
        // Given
        // When
        val recommendation = awaitRecommendation()

        assertThat(recommendation.relatedProducts).isNotEmpty()
        assertThat(recommendation.relatedProducts.map { it.product.name }).isNotEmpty()
    }

    private fun awaitRecommendation(): RelatedProducts {
        val properties = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "e2e-${UUID.randomUUID()}",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl,
        ).toProperties()

        KafkaConsumer<OrderId, RelatedProducts>(
            properties,
            avroSerdes.string.deserializer(),
            avroSerdes.create<RelatedProducts>().deserializer(),
        ).use { consumer ->
            consumer.subscribe(listOf(topicNameBuilder.build(Topic.RELATED_PRODUCTS)))
            val deadline = System.currentTimeMillis() + RECOMMENDATION_TIMEOUT_SECONDS * 1000
            while (System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500))
                    .firstOrNull { it.value().relatedProducts.isNotEmpty() }
                    ?.let { return it.value() }
            }
        }
        error("No recommendation reached ${topicNameBuilder.build(Topic.RELATED_PRODUCTS)} in time")
    }

    private fun get(path: String): HttpResponse<String> = http.send(
        HttpRequest.newBuilder(URI("$recommenderUrl$path")).GET().build(),
        HttpResponse.BodyHandlers.ofString(),
    )
}

private fun env(name: String, default: String) = System.getenv(name) ?: default
