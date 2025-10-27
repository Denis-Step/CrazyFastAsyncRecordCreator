# Repository Guidelines

## Project Structure & Module Organization
- Gradle is configured via `settings.gradle`, `build.gradle`, and the wrapper scripts (`gradlew*`, `gradle/wrapper/**`); run all tasks through the wrapper.
- Java sources live under `src/main/java/com/example/submission`, with `App.java` acting as the Spark entry point. Add new domains as sibling packages to keep module boundaries explicit.
- Tests reside in `src/test/java` and should mirror the main package tree (e.g., `AppTest` exercises `App`).
- Generated artifacts land in `build/`; nothing inside `build/` or `.gradle/` should be committed.

## Build, Test, and Development Commands
- `./gradlew clean build`: compiles Java 17 sources, packages the distribution, and runs the full verification suite.
- `./gradlew run --args="--foo bar"`: launches `com.example.submission.App` with optional arguments; use `-Dspark.master=local[2]` to override the default.
- `./gradlew test`: executes the JUnit 5 suite in an isolated JVM; combine with `--info` when diagnosing Spark planner issues.
- `./gradlew stage`: assembles the distribution (via `installDist`) for container images or deployment archives.

## Coding Style & Naming Conventions
- Use Java 17 language features where they improve clarity (records, text blocks) but keep APIs Java-friendly.
- Indent with 4 spaces, wrap at ~120 columns, and favor expressive method names such as `computeSubmissionMetrics`.
- Keep Spark helper methods `static` and package-private when tests need to exercise them directly.
- Avoid mutable shared state; prefer passing `SparkSession` handles or configuration maps explicitly.

## Testing Guidelines
- Use JUnit 5 (`@Test`, `@BeforeEach`, `@AfterEach`); colocate fixtures next to the classes they verify.
- When asserting Spark results, collect to a bounded list (`collectAsList()`) and assert using the column names (`row.getLong(row.fieldIndex("totalSubmissions"))`).
- Keep Spark master scoped to `local[1]` in tests to minimize resource usage; tune via `App.createSession`.
- Add regression tests whenever you add a transformation or SQL view to protect contracts with Artemis.

## Commit & Pull Request Guidelines
- Follow the conventional summary style from the initial history: `<area>: brief imperative` (e.g., `spark: add submission metric scaffold`).
- Each PR should describe the business change, the impact on Spark jobs (inputs/outputs), and how it was validated (`./gradlew test`, sample job run, etc.).
- Reference tracking tickets or Artemis issue IDs in the body, and include logs/screenshots when altering Spark SQL plans or metrics.

## Spark Configuration Tips
- Default master is `local[*]`; override with `-Dspark.master` for integration tests or cluster submits.
- Keep `spark.sql.shuffle.partitions` low (currently `1`) for local runs; raise it only when benchmarking distributed execution.
- Use `App.computeSubmissionMetrics` as a template for new aggregations: register temp views, keep SQL in multiline text blocks, and cover them with tests.*** End Patch```}!
