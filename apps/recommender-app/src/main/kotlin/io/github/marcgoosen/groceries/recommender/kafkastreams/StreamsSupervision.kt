package io.github.marcgoosen.groceries.recommender.kafkastreams

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse
import java.util.concurrent.atomic.AtomicReference

private val logger = KotlinLogging.logger {}

internal const val STATE_GAUGE = "kafka.streams.state"

/**
 * PENDING_ERROR is the window between a fatal error and the client finishing its teardown. Treating it as alive is
 * what lets a stuck teardown keep reporting a healthy pod, so it counts as dying here.
 */
val State.isDeadOrDying get() = this == State.PENDING_ERROR || hasCompletedShutdown()

/**
 * Makes a failing stream client visible: it shuts the whole client down rather than limping on with fewer threads,
 * logs every state transition, and publishes the current state as a gauge so it can be alerted on.
 */
fun KafkaStreams.superviseWith(registry: MeterRegistry): KafkaStreams = apply {
    val state = AtomicReference(State.CREATED)

    setStateListener { newState, oldState ->
        state.set(newState)
        when {
            newState.isDeadOrDying -> logger.error { "Kafka Streams went from $oldState to $newState" }
            else -> logger.info { "Kafka Streams went from $oldState to $newState" }
        }
    }

    setUncaughtExceptionHandler { throwable ->
        logger.error(throwable) { "A stream thread died; shutting the client down so the pod is replaced" }
        StreamThreadExceptionResponse.SHUTDOWN_CLIENT
    }

    Gauge.builder(STATE_GAUGE) { state.get().ordinal.toDouble() }
        .description("Ordinal of the current KafkaStreams.State")
        .register(registry)
}
