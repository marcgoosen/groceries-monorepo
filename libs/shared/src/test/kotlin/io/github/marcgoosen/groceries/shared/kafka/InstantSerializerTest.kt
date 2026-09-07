package io.github.marcgoosen.groceries.shared.kafka

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema
import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.junit.jupiter.api.Test

@Serializable
private data class Event(@Serializable(with = InstantSerializer::class) val at: Instant)

class InstantSerializerTest {
    private val timestampSchema = Avro.schema(Event.serializer()).getField("at").schema()

    @Test
    fun `It should describe the timestamp as an Avro long carrying timestamp-millis`() {
        // Given
        // When
        val type = timestampSchema.type

        assertThat(type).isEqualTo(Schema.Type.LONG)
        assertThat(timestampSchema.logicalType.name).isEqualTo("timestamp-millis")
    }

    @Test
    fun `It should round-trip an instant`() {
        // Given
        val event = Event(Instant.parse("2026-01-15T09:30:00.123Z"))

        // When
        val roundTripped = Avro.decodeFromByteArray(
            Event.serializer(),
            Avro.encodeToByteArray(Event.serializer(), event),
        )

        assertThat(roundTripped).isEqualTo(event)
    }

    @Test
    fun `It should drop precision finer than a millisecond`() {
        // Given
        val event = Event(Instant.parse("2026-01-15T09:30:00.123456789Z"))

        // When
        val roundTripped = Avro.decodeFromByteArray(
            Event.serializer(),
            Avro.encodeToByteArray(Event.serializer(), event),
        )

        assertThat(roundTripped).isEqualTo(Event(Instant.parse("2026-01-15T09:30:00.123Z")))
    }
}
