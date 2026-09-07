package io.github.marcgoosen.groceries.recommender

import assertk.assertThat
import assertk.assertions.hasMessage
import assertk.assertions.isEqualTo
import assertk.assertions.isFailure
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isSuccess
import ch.qos.logback.classic.LoggerContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

class AppTest {
    @AfterEach
    fun onTearDown() {
        configureLogback("logback-test.xml")
    }

    @Test
    fun `It should apply the logback configuration it is pointed at`() {
        // Given
        // When
        configureLogback("logback-local.xml")

        val context = LoggerFactory.getILoggerFactory() as LoggerContext
        assertThat(context.getLogger("io.github.marcgoosen.groceries").level.toString()).isEqualTo("DEBUG")
        assertThat(context.getLogger("root").getAppender("CONSOLE")).isNotNull()
    }

    @Test
    fun `It should reconfigure logging when pointed at a different file`() {
        // Given
        configureLogback("logback-local.xml")

        // When
        configureLogback("logback.xml")

        val context = LoggerFactory.getILoggerFactory() as LoggerContext
        assertThat(context.getLogger("org.apache.kafka").level.toString()).isEqualTo("INFO")
    }

    @Test
    fun `It should fail when the logback configuration is not on the classpath`() {
        // Given
        // When
        assertThat(runCatching { configureLogback("logback-nonexistent.xml") })
            .isFailure()
            .isInstanceOf(IllegalArgumentException::class)
            .hasMessage("File not found on classpath: logback-nonexistent.xml")
    }

    @Test
    fun `It should apply every logback configuration the application ships`() {
        // Given
        val shipped = listOf("logback.xml", "logback-local.xml", "logback-test.xml")

        // When
        assertThat(runCatching { shipped.forEach { configureLogback(it) } }).isSuccess()
    }
}
