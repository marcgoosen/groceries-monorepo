# Kotlin Project Conventions

Reusable code-style, testing, and working-style guidance for Kotlin projects. Adapt the project-specific sections (build commands, module layout) per repo.

## Pull requests

Every PR follows the template at [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md). Fill in the Summary, Changes, and Testing sections and tick the checklist before requesting review.

Commit messages **and** PR titles follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification — e.g. `feat: add sliding window strategy`, `fix(core): correct epsilon transition compilation`, `chore: bump kafka-streams to 3.9.0`.

## Working style (applies to every task)

### Think before coding
- State assumptions explicitly. If uncertain, ask rather than guess.
- If multiple interpretations exist, surface them — don't silently pick one.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop, name what's confusing, and ask.

### Simplicity first
Minimum code that solves the problem. Nothing speculative.
- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.
- Ask: "Would a senior engineer call this overcomplicated?" If yes, simplify.

### Surgical changes
Touch only what you must. Clean up only your own mess.
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.
- Remove imports/variables/functions that *your* changes made unused; leave pre-existing dead code unless asked.
- The test: every changed line should trace directly to the request.

### Goal-driven execution
Define success criteria, then loop until verified.
- "Add validation" → "Write tests for invalid inputs, then make them pass."
- "Fix the bug" → "Write a test that reproduces it, then make it pass."
- "Refactor X" → "Ensure tests pass before and after."
- For multi-step tasks, state a brief plan with a verification check per step.

## Code style

Clean, concise, no fluff. Functional first, immutable by default.

### Immutability
- **`val` over `var`.** Use `var` only in tight local scope and only when an FP rewrite would be more obscure.
- **Immutable collections** (`List`, `Map`, `Set`) over mutable ones. Prefer `buildList`/`buildMap` over `mutableListOf` accumulators.
- **`data class` / `value class` / `sealed class`** for state. Express change as `copy(...)`, never via setters.

### Functional flow
- **`map`/`filter`/`fold`/`flatMap`** over `for`/`while` loops. Loops only when the imperative shape is genuinely clearer (rare).
- **Expression style**: single-expression functions and `when` expressions over multi-statement blocks where clarity allows.
- **No side effects in expression position.** A function returns *or* it acts; not both.

### Errors as values
- Prefer a **result type** (`Result`, a sealed `Success`/`Error`, `Either`, etc.) at layer boundaries; don't throw across them.
- No `try/catch` in business logic. Wrap external calls at the boundary (HTTP client, IO) and return a result value.
- Reserve exceptions for truly unrecoverable infrastructural failures (e.g. fatal startup). Surface them as an error value at the outermost layer.

### Concurrency
- **Structured concurrency only.** `coroutineScope { async {…}; async {…}.await() }` for fixed parallel sets; a `parallelMap`-style helper (optionally `Semaphore`-bounded) for collection-wide work. No fire-and-forget `launch`.
- A bounding `Semaphore` of `null` means unbounded — gate it behind `count.takeIf { it > 0 }?.let { Semaphore(it) }`.

### Types over primitives
- **Value classes** for identifiers and bounded values (`Email`, `UserId`, `Money`). No raw `String`/`Int` for domain concepts.
- **Sealed hierarchies** for things-with-variants. Pattern-match exhaustively in `when`.
- **Nullable types** (`T?`) over sentinel values. Avoid `Optional<T>`.

### `when` expressions
- Always exhaustive on sealed types and enums. No `else -> null` / `else -> ""` fallbacks — let the compiler force update sites when variants change.
- Enum-to-enum mapping is always an explicit `when`, never `valueOf`/`toString` reflection.

### Function shape
- **≤3 parameters**; otherwise take a `data class`. Prefer fewer parameters regardless.
- **Call functions positionally — no named arguments.** Reserve named arguments for `data class` / `value class` construction, where they document fields. Exception: a function with several optional/defaulted params — name the args you pass so omitted defaults stay legible. If you reach for named args to make a call readable, that's usually a sign the function has too many parameters; take a `data class` instead.
- **Extension functions** over utility classes or companion-object helpers.

### Optional inputs
- Default parameter values over caller-side null checks: `= emptySet()`, `= MyOptions()`, `= null`.
- Convenience overloads delegate to a single `private` method with optional params, rather than duplicating logic.
- `.ifEmpty { null }` / `.takeUnless { … }` to omit empty collections from output instead of emitting empty arrays.

### Naming
- **Mappers**: `to*()` extension functions in the relevant `mapper/` package. `@JvmName` only when overload signatures collide.
- **Files**: `*Extensions.kt` for grouped extension functions on a single type. No `Utils` / `Helpers` catch-alls.

### Imports
- **No inline fully-qualified names.** Any class, interface, or annotation you reference must have an `import` at the top of the file — never written as `com.foo.bar.Thing` or `@com.foo.bar.Annotation` at the use site. Applies even when it's "just one occurrence." The rule has no carve-outs.

### Logging
- Structured logging via lazy lambdas (`logger.warn { "..." }`), not string concatenation.
- Log a one-line `warn` at error-raise sites where it adds debugging value before returning the error. Don't log success paths.

### Comments
- **Default to zero comments — express intent in code.** Before writing one, extract a well-named function, `val`, or type that makes it unnecessary; a good name outlives a comment, which goes stale and costs maintenance.
- A comment is a last resort, earned only by a *why* that genuinely cannot be expressed in code — a workaround, an external constraint, a surprising invariant. When in doubt, leave it out.
- **Never restate code or duplicate metadata** — no comment that repeats an annotation, exception, or log message (`@Disabled("reason")`, `@Deprecated(message = ...)`, `error("reason")`).
- **Describe the current state, not the change.** No history or diff narration in code or KDoc — no `previously` / `now` / `no longer` / `used to` / `replaces X` / `renamed from`. That belongs in the commit message and PR description.

### What "no fluff" means
- No defensive validation for cases that can't happen. Trust internal callers; validate at system boundaries (HTTP routes, external APIs).
- No backwards-compatibility shims for internal-only changes.
- No helpers introduced "for symmetry" or "in case we need it later." Three similar lines beat a premature abstraction.
- No half-finished implementations or placeholder branches.

## Testing

### File organization
- **One test file per production class**, named `ClassNameTest.kt`, mirroring the production package under `src/test/kotlin/...`.
- **No catch-all test files** (`MiscTest.kt`). One class → one file.
- **Every production class with real logic has a matching test file.** Extensions → `FooExtensionsTest.kt`; mappers → mapper test.
- **Skip trivial data classes.** Asserting Kotlin's generated getters/`equals`/defaults is zero signal. (Exception: a `@Serializable` data class with defaulted fields — add the [all-defaults round-trip](#pattern-all-defaults-round-trip) to keep coverage honest.)

### Naming
- Every test method starts with `` `It should ...` ``.
- Describe observable **behaviour**, not implementation.
  - ✓ `` `It should reject invalid email format` ``, `` `It should persist user data` ``
  - ✗ `` `It should throw ValidationException` ``, `` `It should call repository save method` ``
- Be specific. "correctly", "works", "handle error" are not specific.
- Name tests in terms of domain types, not wire/DTO types.

### Structure
- Body always uses `// Given` and `// When` comments; the assertions follow the `// When` block.
  ```kotlin
  // Given
  // When

  assertThat(result).isEqualTo(expected)
  ```
  - Combine When/Then when inseparable (e.g. exception assertions).
  - No trailing descriptions after the keywords — let the code speak.
- **One When/Then sequence per test.** If you need more, split the test.
- `@Nested inner class` to group tests by scenario or public method.

### Fixtures
- **No `a`/`an` article prefix.** `val deal`, not `val aDeal`.
- **Realistic values.** A real domain object should have plausible data; use `emptyList()` only when the test is specifically about the empty case.
- **Class-level `private val`** for shared fixtures; local `.copy(...)` for per-test variations.
- **No fixture factory functions** — build inline or via `private val` + `.copy`.

### Assertions
- **Use `assertk`** (`assertThat(...).isEqualTo(...)`, `.isNull()`, `.hasSize(n)`, etc.) — never JUnit's `assertEquals` or `kotlin.test.assertTrue`/`assertNotNull`. assertk failures render the full `toString()` of both sides.
- **Whole data-class equality**, never field-by-field decomposition.
  - ✓ `assertThat(result).isEqualTo(expected)`
  - ✗ `assertThat(result.x).isEqualTo(...); assertThat(result.y).isEqualTo(...)`
- **Reuse the production mapper** when the expected value is what the mapper produces — avoids hand-maintaining many-field expected fixtures that break on every new field.
- **Structural, not string-based.** Deserialize JSON before asserting; use `.contains("...")` for partial text only. Never assert on raw JSON substrings like `"\"firstName\""`.
- **Use assertk's typed assertions instead of boolean conditions.** Wrapping a stdlib predicate in `isTrue()` discards context — the message is just "expected true but was false". Use collection/property assertions so failures show the offending value:
  ```kotlin
  // ✗ — failure says "expected true" with no context
  assertThat(deals.all { it.isHidden }).isTrue()
  assertThat(deals.any { it.name == "Free Fries" }).isTrue()

  // ✓ — failure shows which element violated the assertion
  assertThat(deals).each { it.prop(Deal::isHidden).isTrue() }
  assertThat(deals).any { it.prop(Deal::name).isEqualTo("Free Fries") }
  ```

### Parameterized tests
- `@ParameterizedTest` with `@MethodSource` when enumerating enum-like variants.
- `@CsvSource` for scalar tuples readable inline.
- Use `@ParameterizedTest(name = "...")` templates so IDE output includes the meaningful inputs.
- Parametrize when each row is a distinct behaviour/edge case. Don't inflate rows just because you can.
- **`@MethodSource` inside `@Nested inner class`** must use a fully-qualified path (e.g. `@MethodSource("com.example.OuterTest#cases")`) — `inner class` bodies can't declare a `companion object`.
- **In KMP `commonTest` (`kotlin.test`) there is no `@ParameterizedTest`.** Use a single `@Test` that iterates a `Map<Input, Expected>` and asserts row-by-row.

### Redundancy
Each rule of behaviour lives in exactly one place. Duplicated coverage slows the suite without catching extra regressions.
- **Error-table mappings** belong to *one* canonical test. Other layers keep **one smoke test** that errors propagate at all — not every row.
- **Boundary/adapter tests** focus on their own behaviour (business rules, fallbacks, message composition, ordering) — not the error table.
- **Route/controller tests** cover a happy path (status + body) + param forwarding + one error-propagation smoke. Not every 4xx/5xx.
- **End-to-end tests** already cover the happy path — don't duplicate it one layer down.
- **Delegation tests** must stub the collaborator with a concrete return and assert the *result* (mapped/filtered as the unit transforms it). A bare `verify { collaborator.x() }` proves the call happened, not that the result is correct.
- **Collapse overlapping delegation tests** — two tests proving one fact each about the same happy path are one test.

### Composed mappers / functions
A top-level function that composes child functions:
- **Parent `@Nested`**: one whole-object happy path + composition logic *specific to the parent* (its own fallbacks/coercions). Not every child edge case.
- **Child `@Nested`** per internal function: all of that function's edge cases live here.
- **Shared child** (called by >1 parent): its edge-case tests live in **exactly one** test file; the other parent stays happy-path only.
- **Smell test.** A bug in one child should fail one test in one file. If it fails six tests across two files, coverage is duplicated.

### (De)serialization tests
**Guiding principle: match the test direction to the runtime primary use.** Round-trip both directions in one test for full coverage; make the direction the production code actually uses at runtime the *primary* assertion.

| Artefact | Primary runtime use | Test direction |
| --- | --- | --- |
| Inbound response body | Deserialize JSON → model | **Deserialize-first** |
| Outbound request body | Serialize model → JSON | **Serialize-first** |
| API response you emit | Serialize model → wire | **Serialize-first** |
| API request you receive | Deserialize wire → model | **Deserialize-first** |
| Enum | Both at runtime | `@ParameterizedTest` round-trip |

Common rules:
- **Single `Json` instance as a class property**, reused: `private val json = Json { prettyPrint = true }`.
- **No `ignoreUnknownKeys` in model/mapper tests** — you control the payloads, so an unknown key is a bug. (Client tests are the exception: mirror the production HttpClient `Json` config.)
- **`prettyPrint = true`** so payloads are readable and assertable verbatim.
- **`encodeDefaults = false` is the default** — fields equal to their declared default are omitted; the round-trip payload must reflect that.
- **Complete standalone JSON per edge case** — never build edge payloads via `.replace()`. Keep edge payloads minimal: required fields + the field under test.
- **Whole-model assertions** — `isEqualTo(expectedWhole)`, never property-by-property.

#### Pattern: deserialize-first
```kotlin
class ExampleResponseTest {
    private val json = Json { prettyPrint = true }

    private val payload = """
        {
            "id": 123,
            "name": "Test",
            "tags": ["a", "b"]
        }
    """.trimIndent()

    private val exampleResponse = ExampleResponse(id = 123L, name = "Test", tags = listOf("a", "b"))

    @Test
    fun `It should deserialize and round-trip`() {
        // When
        val deserialized = json.decodeFromString<ExampleResponse>(payload)

        assertThat(deserialized).isEqualTo(exampleResponse)

        val serialized = json.encodeToString(deserialized)
        assertThat(serialized).isEqualTo(payload)
    }
}
```

#### Pattern: serialize-first
```kotlin
class ExampleRequestTest {
    private val json = Json { prettyPrint = true }

    private val exampleRequest = ExampleRequest(email = "test@example.com", name = "Test")

    private val payload = """
        {
            "email": "test@example.com",
            "name": "Test"
        }
    """.trimIndent()

    @Test
    fun `It should serialize and round-trip`() {
        // When
        val serialized = json.encodeToString(exampleRequest)

        assertThat(serialized).isEqualTo(payload)

        val deserialized = json.decodeFromString<ExampleRequest>(serialized)
        assertThat(deserialized).isEqualTo(exampleRequest)
    }
}
```

#### Pattern: enums
One test class per enum. `@ParameterizedTest` + `@MethodSource` listing every known variant; round-trip each. Use plain `Json` (no `prettyPrint`). For lenient enums (return `null` on unknown), add a separate test asserting the unknown path is `null`.

#### Pattern: all-defaults round-trip
Add **one** of these per `@Serializable` model with two-or-more defaulted fields — it covers both serializer and deserializer synthetics in a single test.

**Why:** kotlinx-serialization generates a synthetic constructor with two branches per defaulted field (absent → default, present → decoded). A full-payload round-trip only exercises "present"; coverage tools count each branch and tank even at 100% line coverage. With `encodeDefaults = false`, an all-defaults instance serializes to `{}` and `{}` deserializes back — firing every default branch in both directions.

```kotlin
@Test
fun `It should round-trip a default-valued instance through the empty-object payload`() {
    val empty = MyModel()

    // When
    val serialized = json.encodeToString(empty)

    assertThat(serialized).isEqualTo("{}")

    val deserialized = json.decodeFromString<MyModel>(serialized)
    assertThat(deserialized).isEqualTo(empty)
}
```
Skip when the model has no defaulted fields. If some fields are required, pass dummy values for them — the synthetic still covers every defaulted-field branch.

### HTTP client tests
- **One test per client method.** Assert method, path, headers, and response together.
- For POST/PUT, assert the request body equals `json.encodeToString(inputModel)`.
- Pass test headers and verify they're forwarded to the external service.
- **Don't duplicate model edge-case coverage** — model tests already verify serialization.
- Test optional query-parameter variants (present vs absent) in separate tests.
- Test at least one non-2xx response returns the correct error type.

### Review checklist
- [ ] Every new/changed production class has a `ClassNameTest.kt`
- [ ] Tests named `` `It should ...` ``, describing behaviour
- [ ] Given/When/Then comments present (skip trivial Given)
- [ ] One When/Then per test
- [ ] Whole-object `isEqualTo`, not field-by-field
- [ ] Production mapper reused where `expected == mapper(fixture)`
- [ ] Fixtures named without `a`/`an` prefix, using realistic values
- [ ] Error-table coverage in one place, not repeated per layer
- [ ] Delegation tests stub returns and assert result equality (not just `verify`)
- [ ] Each child mapper's edge cases in a dedicated `@Nested`; shared children tested once
- [ ] Parameterised with `@MethodSource` when enumerating variants; fully-qualified path from `@Nested inner class`
- [ ] No zero-signal tests (asserting compiler output)
- [ ] Serialization tests match the runtime primary direction
- [ ] Single `Json { prettyPrint = true }` property; no `ignoreUnknownKeys` in model tests
- [ ] Edge-case payloads standalone, not built via `.replace()`
- [ ] Every `@Serializable` model with defaulted fields has the all-defaults round-trip (or a stated reason)
- [ ] Formatter (`ktlintFormat` or equivalent) and the test suite pass
