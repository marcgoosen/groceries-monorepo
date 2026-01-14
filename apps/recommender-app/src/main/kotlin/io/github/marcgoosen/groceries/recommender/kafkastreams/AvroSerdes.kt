package io.github.marcgoosen.groceries.recommender.kafkastreams

import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.kafka.confluent.SpecificAvro4kKafkaSerde

@OptIn(ExperimentalAvro4kApi::class)
class AvroSerdes(val serdeConfig: Map<String, String>) {
    inline fun <reified T : Any> create(isKey: Boolean = false) = SpecificAvro4kKafkaSerde<T>().also {
        it.configure(serdeConfig, isKey)
    }

    val string by lazy { create<String>(true) }
}
