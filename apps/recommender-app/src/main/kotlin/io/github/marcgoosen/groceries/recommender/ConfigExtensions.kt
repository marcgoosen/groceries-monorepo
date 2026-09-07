package io.github.marcgoosen.groceries.recommender

private const val MASK = "********"

private val secretKeyPattern = Regex("password|secret|credential|jaas|auth\\.user\\.info", RegexOption.IGNORE_CASE)

private val String.isSecret get() = secretKeyPattern.containsMatchIn(this)

fun Config.scramble() = copy(
    kafka = kafka.mapValues { (key, value) ->
        when {
            key.isSecret && value.isNotBlank() -> MASK
            else -> value
        }
    },
)
