package io.github.marcgoosen.groceries.recommender

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource
import io.github.marcgoosen.groceries.recommender.kafkastreams.AvroSerdes
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopicCreator
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopicNameBuilder
import io.github.marcgoosen.groceries.recommender.kafkastreams.TopologyBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder

private val logger = KotlinLogging.logger {}

object Dependencies {
    val json by lazy {
        Json {
            prettyPrint = true
            ignoreUnknownKeys = true
        }
    }

    val config by lazy {
        ConfigLoaderBuilder.default()
            //            .addFileSource(
            //
            // "${Paths.get("").toAbsolutePath()}/apps/rtg-cycle-time/config/application-local.yaml",
            //                optional = true,
            //                allowEmpty = true,
            //            )
            .addResourceSource("/application.yaml")
            .build()
            .loadConfigOrThrow<Config>()
    }

    val prometheusRegistry by lazy {
        PrometheusMeterRegistry(PrometheusConfig.DEFAULT).also {
            it.config().onMeterRegistrationFailed { meterId, reason ->
                logger.warn { "Meter registration failed for $meterId: $reason" }
            }
        }
    }

    val topicNameBuilder by lazy { TopicNameBuilder(config.topics.name) }

    val avroSerdes by lazy { AvroSerdes(config.kafka) }
    val builder by lazy { StreamsBuilder() }

    val topology by lazy {
        TopologyBuilder(
            builder,
            avroSerdes,
            topicNameBuilder,
        )
            .build()
    }

    val streams by lazy { KafkaStreams(topology, config.kafka.toProperties()) }

    val admin: Admin by lazy { Admin.create(config.kafka.toProperties()) }

    val topicCreator by lazy {
        TopicCreator(
            admin,
            topicNameBuilder,
        )
    }

    val simulator by lazy {
        Simulator(
            config.kafka,
            avroSerdes,
            topicNameBuilder,
        )
    }
}
