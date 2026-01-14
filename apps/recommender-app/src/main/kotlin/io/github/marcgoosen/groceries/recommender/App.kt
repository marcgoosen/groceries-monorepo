package io.github.marcgoosen.groceries.recommender

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import io.github.marcgoosen.groceries.recommender.routing.configureHealth
import io.github.marcgoosen.groceries.recommender.routing.configurePrometheus
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.engine.addShutdownHook
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import org.slf4j.LoggerFactory

private val logger = KotlinLogging.logger {}

fun main(): Unit = with(Dependencies) {
    configureLogback(config.main.logbackConfigFile)

    logger.atInfo { message = "Starting ${config.main.applicationId}" }
    logger.atInfo {
        message = "I got this config"
        payload = mapOf("config" to json.encodeToString(config.scramble()))
    }

    if (config.main.createTopics) {
        topicCreator.createTopics(config.topics.create)
    }

    if (config.main.startSimulator) {
        simulator.start()
    }

    logger.info { topology.describe() }
    streams.cleanUp()
    streams.start()

    val engine =
        embeddedServer(Netty, config.main.port) {
            install(ContentNegotiation) { json() }
            configureHealth(streams)
            configurePrometheus(prometheusRegistry, streams)
        }

    engine.addShutdownHook {
        println("Closing, cleaning up...")
        streams.close()
        simulator.close()
    }

    engine.start(wait = true)
}

/** See: https://logback.qos.ch/manual/configuration.html#joranDirectly */
fun configureLogback(filePath: String) {
    loadConfigFile(filePath).use { configFile ->
        JoranConfigurator()
            .apply {
                context = LoggerFactory.getILoggerFactory() as LoggerContext
                (LoggerFactory.getILoggerFactory() as LoggerContext).reset()
            }
            .doConfigure(configFile)
    }
}

private fun loadConfigFile(filePath: String) = Thread.currentThread().contextClassLoader.getResourceAsStream(filePath)
    ?: throw IllegalArgumentException("File not found on classpath: $filePath")

fun Config.scramble() = copy(
    kafka =
    kafka +
        mapOf(
            "sasl.jaas.config" to "********",
        ),
)
