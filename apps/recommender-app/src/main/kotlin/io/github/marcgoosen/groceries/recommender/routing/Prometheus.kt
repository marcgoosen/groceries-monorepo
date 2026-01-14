package io.github.marcgoosen.groceries.recommender.routing

import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.metrics.micrometer.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
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
