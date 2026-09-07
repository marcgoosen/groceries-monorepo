package io.github.marcgoosen.groceries.recommender.routing

import assertk.assertThat
import assertk.assertions.isEqualTo
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.testing.testApplication
import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.streams.KafkaStreams
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class HealthTest {
    @ParameterizedTest(name = "{0}")
    @EnumSource(value = KafkaStreams.State::class, names = ["RUNNING", "REBALANCING"])
    fun `It should report the service ready while the streams are running`(state: KafkaStreams.State) = withHealth(
        state,
    ) { client ->
        // Given
        // When
        val response = client.get("/health/readiness")

        assertThat(response.status).isEqualTo(HttpStatusCode.OK)
        assertThat(response.bodyAsText()).isEqualTo("""{"status":"UP"}""")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(
        value = KafkaStreams.State::class,
        names = ["CREATED", "PENDING_SHUTDOWN", "PENDING_ERROR", "NOT_RUNNING", "ERROR"],
    )
    fun `It should report the service not ready while the streams are not running`(state: KafkaStreams.State) =
        withHealth(state) { client ->
            // Given
            // When
            val response = client.get("/health/readiness")

            assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
            assertThat(response.bodyAsText()).isEqualTo("""{"status":"DOWN"}""")
        }

    @Test
    fun `It should report the service alive while the streams have not shut down`() = withHealth(
        KafkaStreams.State.REBALANCING,
    ) { client ->
        // Given
        // When
        val response = client.get("/health/liveness")

        assertThat(response.status).isEqualTo(HttpStatusCode.OK)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = KafkaStreams.State::class, names = ["NOT_RUNNING", "ERROR"])
    fun `It should report the service dead once the streams have shut down`(state: KafkaStreams.State) =
        withHealth(state) { client ->
            // Given
            // When
            val response = client.get("/health/liveness")

            assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
            assertThat(response.bodyAsText()).isEqualTo("""{"status":"DOWN"}""")
        }

    private fun withHealth(state: KafkaStreams.State, block: suspend (HttpClient) -> Unit) = testApplication {
        val streams = mockk<KafkaStreams> { every { state() } returns state }

        application {
            install(ContentNegotiation) { json() }
            configureHealth(streams)
        }

        block(client)
    }
}
