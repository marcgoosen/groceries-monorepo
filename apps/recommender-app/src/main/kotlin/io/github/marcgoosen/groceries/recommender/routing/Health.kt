package io.github.marcgoosen.groceries.recommender.routing

import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.apache.kafka.streams.KafkaStreams

fun Application.configureHealth(streams: KafkaStreams) {
    routing {
        get("/health/liveness") {
            when {
                !streams.state().hasCompletedShutdown() -> {
                    call.response.status(HttpStatusCode.OK)
                    call.respond(mapOf("status" to "UP"))
                }

                else -> {
                    call.response.status(HttpStatusCode.ServiceUnavailable)
                    call.respond(mapOf("status" to "DOWN"))
                }
            }
        }
        get("/health/readiness") {
            when {
                streams.state().isRunningOrRebalancing -> {
                    call.response.status(HttpStatusCode.OK)
                    call.respond(mapOf("status" to "UP"))
                }

                else -> {
                    call.response.status(HttpStatusCode.ServiceUnavailable)
                    call.respond(mapOf("status" to "DOWN"))
                }
            }
        }
    }
}
