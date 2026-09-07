package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isEqualTo
import assertk.assertions.isFailure
import assertk.assertions.isInstanceOf
import assertk.assertions.isSuccess
import io.github.marcgoosen.groceries.recommender.Config
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.TopicExistsException
import org.junit.jupiter.api.Test
import java.util.concurrent.ExecutionException

class TopicCreatorTest {
    private val admin = mockk<Admin>()
    private val topicNameBuilder = TopicNameBuilder(mapOf("orders" to "groceries.orders.v1"))
    private val topicCreator = TopicCreator(admin, topicNameBuilder)

    private val create = mapOf(
        "orders" to Config.TopicConfig(
            partitions = 3,
            replicationFactor = 2,
            configs = mapOf("cleanup.policy" to "compact"),
        ),
    )

    @Test
    fun `It should create each configured topic under its resolved name`() {
        // Given
        val newTopics = slot<List<NewTopic>>()
        every { admin.createTopics(capture(newTopics)) } returns createTopicsResult(completedFuture())

        // When
        topicCreator.createTopics(create)

        assertThat(newTopics.captured).isEqualTo(
            listOf(
                NewTopic("groceries.orders.v1", 3, 2)
                    .configs(mapOf("cleanup.policy" to "compact")),
            ),
        )
    }

    @Test
    fun `It should accept a topic that already exists`() {
        // Given
        every { admin.createTopics(any<List<NewTopic>>()) } returns
            createTopicsResult(failedFuture(TopicExistsException("exists")))

        // When
        assertThat(runCatching { topicCreator.createTopics(create) }).isSuccess()
    }

    @Test
    fun `It should fail when a topic could not be created`() {
        // Given
        every { admin.createTopics(any<List<NewTopic>>()) } returns
            createTopicsResult(failedFuture(TopicAuthorizationException("not allowed")))

        // When
        assertThat(runCatching { topicCreator.createTopics(create) })
            .isFailure()
            .isInstanceOf(IllegalStateException::class)
            .hasMessage("Could not create topics: [groceries.orders.v1]")
    }

    @Test
    fun `It should reject a configured topic that maps to no known topic`() {
        // Given
        // When
        assertThat(runCatching { topicCreator.createTopics(mapOf("deliveries" to Config.TopicConfig())) })
            .isFailure()
            .isInstanceOf(IllegalArgumentException::class)
            .hasMessage("Unknown topic: deliveries")
    }

    private fun createTopicsResult(future: KafkaFuture<Void>) = mockk<CreateTopicsResult> {
        every { values() } returns mapOf("groceries.orders.v1" to future)
    }

    private fun completedFuture(): KafkaFuture<Void> = KafkaFuture.completedFuture(null)

    private fun failedFuture(cause: Throwable) = mockk<KafkaFuture<Void>> {
        every { get() } throws ExecutionException(cause)
    }
}
