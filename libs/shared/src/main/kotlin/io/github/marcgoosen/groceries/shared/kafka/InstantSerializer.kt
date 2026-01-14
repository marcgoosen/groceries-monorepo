package io.github.marcgoosen.groceries.shared.kafka

import com.github.avrokotlin.avro4k.AvroDecoder
import com.github.avrokotlin.avro4k.AvroEncoder
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.serializer.AvroSerializer
import com.github.avrokotlin.avro4k.serializer.SchemaSupplierContext
import kotlinx.datetime.Instant
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import kotlin.jvm.javaClass

class InstantSerializer : AvroSerializer<Instant>(Instant::javaClass.name) {
    override fun serializeAvro(encoder: AvroEncoder, value: Instant) {
        encoder.encodeLong(value.toEpochMilliseconds())
    }

    override fun deserializeAvro(decoder: AvroDecoder): Instant = Instant.fromEpochMilliseconds(decoder.decodeLong())

    @OptIn(ExperimentalAvro4kApi::class)
    override fun getSchema(context: SchemaSupplierContext): Schema {
        val schema = SchemaBuilder.builder().longType()
        return LogicalTypes.timestampMillis().addToSchema(schema)
    }
}
