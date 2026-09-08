package io.github.marcgoosen.groceries.recommender.kafkastreams

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isTrue
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class StreamsSupervisionTest {
    private val registry = SimpleMeterRegistry()

    private val stateListener = slot<KafkaStreams.StateListener>()
    private val exceptionHandler = slot<StreamsUncaughtExceptionHandler>()

    private val streams = mockk<KafkaStreams> {
        every { setStateListener(capture(stateListener)) } returns Unit
        every { setUncaughtExceptionHandler(capture(exceptionHandler)) } returns Unit
    }

    @Test
    fun `It should shut the whole client down when a stream thread dies`() {
        // Given
        streams.superviseWith(registry)

        // When
        val response = exceptionHandler.captured.handle(IllegalStateException("the store is gone"))

        assertThat(response).isEqualTo(StreamThreadExceptionResponse.SHUTDOWN_CLIENT)
    }

    @Test
    fun `It should report the state it was last told about`() {
        // Given
        streams.superviseWith(registry)

        // When
        stateListener.captured.onChange(State.RUNNING, State.REBALANCING)

        assertThat(gaugeValue()).isEqualTo(State.RUNNING.ordinal.toDouble())
    }

    @Test
    fun `It should report the created state before anything has happened`() {
        // Given
        // When
        streams.superviseWith(registry)

        assertThat(gaugeValue()).isEqualTo(State.CREATED.ordinal.toDouble())
    }

    @Test
    fun `It should follow the state all the way into error`() {
        // Given
        streams.superviseWith(registry)

        // When
        listOf(
            State.REBALANCING to State.CREATED,
            State.RUNNING to State.REBALANCING,
            State.PENDING_ERROR to State.RUNNING,
            State.ERROR to State.PENDING_ERROR,
        ).forEach { (new, old) -> stateListener.captured.onChange(new, old) }

        assertThat(gaugeValue()).isEqualTo(State.ERROR.ordinal.toDouble())
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = State::class, names = ["PENDING_ERROR", "ERROR", "NOT_RUNNING"])
    fun `It should treat a client that is dead or on its way out as dying`(state: State) {
        // Given
        // When
        val deadOrDying = state.isDeadOrDying

        assertThat(deadOrDying).isTrue()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = State::class, names = ["CREATED", "REBALANCING", "RUNNING", "PENDING_SHUTDOWN"])
    fun `It should not treat a starting, running or gracefully stopping client as dying`(state: State) {
        // Given
        // When
        val deadOrDying = state.isDeadOrDying

        assertThat(deadOrDying).isFalse()
    }

    private fun gaugeValue() = registry.get(STATE_GAUGE).gauge().value()
}
