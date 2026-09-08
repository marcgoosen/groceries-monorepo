package io.github.marcgoosen.groceries.recommender.routing

import io.github.marcgoosen.groceries.recommender.kafkastreams.isDeadOrDying
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import org.apache.kafka.streams.KafkaStreams

private val up = mapOf("status" to "UP")
private val down = mapOf("status" to "DOWN")

fun Application.configureHealth(streams: KafkaStreams) {
    routing {
        get("/health/liveness") {
            when {
                streams.state().isDeadOrDying -> call.respond(HttpStatusCode.ServiceUnavailable, down)
                else -> call.respond(HttpStatusCode.OK, up)
            }
        }
        get("/health/readiness") {
            when {
                streams.state().isRunningOrRebalancing -> call.respond(HttpStatusCode.OK, up)
                else -> call.respond(HttpStatusCode.ServiceUnavailable, down)
            }
        }
    }
}
