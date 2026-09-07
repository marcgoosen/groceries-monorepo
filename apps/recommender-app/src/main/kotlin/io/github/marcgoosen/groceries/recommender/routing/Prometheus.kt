package io.github.marcgoosen.groceries.recommender.routing

import io.ktor.http.ContentType
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import io.micrometer.core.instrument.binder.logging.LogbackMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.apache.kafka.streams.KafkaStreams

fun Application.configurePrometheus(prometheusRegistry: PrometheusMeterRegistry, kafkaStreams: KafkaStreams) {
    install(MicrometerMetrics) {
        registry = prometheusRegistry
        meterBinders =
            listOf(
                JvmMemoryMetrics(),
                JvmGcMetrics(),
                ProcessorMetrics(),
                LogbackMetrics(),
                KafkaStreamsMetrics(kafkaStreams),
            )
    }
    routing {
        get("/prometheus") {
            call.respondText(prometheusRegistry.scrape(), ContentType.Text.Plain)
        }
    }
}
