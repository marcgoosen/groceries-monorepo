package io.github.marcgoosen.groceries.recommender.routing

import assertk.assertThat
import assertk.assertions.contains
import assertk.assertions.isEqualTo
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.streams.KafkaStreams
import org.junit.jupiter.api.Test

class PrometheusTest {
    private val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    private val streams = mockk<KafkaStreams>(relaxed = true) {
        every { metrics() } returns emptyMap()
    }

    @Test
    fun `It should expose the registry as a Prometheus scrape`() = withPrometheus {
        // Given
        // When
        val response = client.get("/prometheus")

        assertThat(response.status).isEqualTo(HttpStatusCode.OK)
        assertThat(response.contentType()?.withoutParameters()).isEqualTo(ContentType.Text.Plain)
    }

    @Test
    fun `It should report the JVM and process metrics it binds`() = withPrometheus {
        // Given
        // When
        val scrape = client.get("/prometheus").bodyAsText()

        assertThat(scrape).contains("jvm_memory_used_bytes")
        assertThat(scrape).contains("jvm_gc")
        assertThat(scrape).contains("logback_events_total")
    }

    @Test
    fun `It should count the requests it served`() = withPrometheus {
        // Given
        client.get("/prometheus")

        // When
        val scrape = client.get("/prometheus").bodyAsText()

        assertThat(scrape).contains("ktor_http_server_requests")
    }

    private fun withPrometheus(block: suspend ApplicationTestBuilder.() -> Unit) = testApplication {
        application { configurePrometheus(registry, streams) }
        block()
    }
}
