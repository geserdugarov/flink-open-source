# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Flink — a distributed stream and batch processing framework. Version 2.3-SNAPSHOT, Maven multi-module project with 39+ modules.

## Build Commands

Prerequisites: Java 11/17/21 and Maven 3.8.6 (use the included Maven wrapper `./mvnw`).

```bash
# Full build (Java 17 default, skipping tests)
./mvnw clean package -DskipTests -Djdk17 -Pjava17-target

# Java 11 / Java 21 variants
./mvnw clean package -DskipTests -Djdk11 -Pjava11-target
./mvnw clean package -DskipTests -Djdk21 -Pjava21-target

# Fast build (skip QA checks and web UI)
./mvnw clean package -DskipTests -Dfast -Pskip-webui-build

# Build a single module (with dependencies already built)
./mvnw clean package -DskipTests -pl flink-runtime

# Build a single module and its dependencies
./mvnw clean package -DskipTests -pl flink-runtime -am
```

## Running Tests

Test naming conventions determine execution phase:
- **Unit tests**: classes matching `**/*Test.*` — run during `test` phase
- **Integration tests**: all other test classes (typically `**/*ITCase.*`) — run during `integration-test` phase

```bash
# Run all tests in a module
./mvnw verify -pl flink-runtime

# Run a single test class
./mvnw test -pl flink-runtime -Dtest=TaskExecutorTest

# Run a single test method
./mvnw test -pl flink-runtime -Dtest=TaskExecutorTest#testMethod

# Run integration tests in a module
./mvnw verify -pl flink-runtime -Dtest=none -DfailIfNoTests=false
```

## Code Formatting and Quality

- **Java formatting**: Spotless with Google Java Format (AOSP style). Run `./mvnw spotless:apply` to format, or `./mvnw spotless:check` to verify.
- **Scala formatting**: Spotless with scalafmt (config in `.scalafmt.conf`).
- **Checkstyle**: Config at `tools/maven/checkstyle.xml` with suppressions in `tools/maven/suppressions.xml`. Note: some modules (flink-core, flink-optimizer, flink-runtime) are not covered by checkstyle enforcement but should still follow the rules.
- **License headers**: Apache 2.0 license header required on all source files. Verified by Apache RAT plugin.

```bash
# Check formatting
./mvnw spotless:check

# Auto-format code
./mvnw spotless:apply

# Run checkstyle
./mvnw checkstyle:check -pl flink-runtime
```

## Architecture

### Module Layers (top to bottom)

**API Layer** — user-facing interfaces:
- `flink-core-api` / `flink-core` — fundamental types, configuration, type system
- `flink-datastream-api` / `flink-datastream` — new DataStream API (contracts vs implementation)
- `flink-streaming-java` — streaming operators and transformations
- `flink-table/flink-table-api-java` — Table/SQL API for Java
- `flink-clients` — job submission client

**Table/SQL Subsystem** — `flink-table/` contains 21 sub-modules:
- `flink-sql-parser` — ANSI SQL parsing
- `flink-table-planner` — query optimization (Calcite-based), code generation
- `flink-table-runtime` — runtime operator implementations
- `flink-sql-client` / `flink-sql-gateway` — SQL tooling

**Runtime Layer** — `flink-runtime` is the execution engine heart with 55+ packages:
- `dispatcher/` — job submission and cluster coordination
- `jobmaster/` — per-job execution management
- `executiongraph/` — execution plan DAG representation
- `taskmanager/` — task execution on worker nodes
- `io/network/` — network data transfer and buffering
- `checkpoint/` — checkpointing and fault tolerance
- `shuffle/` — data shuffling between operators
- `scheduler/` — job scheduling strategies
- `state/` — state backend interfaces

**Deployment** — `flink-kubernetes`, `flink-yarn`, `flink-container`

**State Backends** — `flink-state-backends/` (RocksDB, Forst, changelog, heap-spillable)

**Connectors & Formats** — `flink-connectors/` (base, datagen, files) and `flink-formats/` (Avro, JSON, Parquet, CSV, ORC, Protobuf). Most specialized connectors (Kafka, JDBC, etc.) are in separate repositories.

**Python** — `flink-python/pyflink/` bridges to the JVM via `java_gateway.py`

### Testing Infrastructure

- `flink-test-utils-parent` — shared test harnesses and MiniCluster utilities
- `flink-tests` — cross-module integration tests
- `flink-end-to-end-tests` — 35+ end-to-end test scenarios
- `flink-architecture-tests` — ArchUnit-based architectural constraint enforcement

### Key Architectural Patterns

- **API/Implementation separation**: Public API modules (`*-api`) are separate from implementations (e.g., `flink-datastream-api` vs `flink-datastream`)
- **Classloader isolation**: RPC layer and table planner use classloader isolation (loader bundles in `flink-rpc/`)
- **SPI plugin architecture**: Connectors, formats, state backends, and metrics reporters are loaded via Service Provider Interface
- **Shaded dependencies**: Critical libraries (Jackson, Netty, Guava, ASM) are shaded to avoid classpath conflicts

## Key Build Profiles

| Profile | Purpose |
|---|---|
| `java11-target` / `java17-target` / `java21-target` | Target Java version for compilation |
| `skip-webui-build` | Skip Angular web dashboard compilation |
| `scala-2.12` | Scala version (active by default) |
| `check-convergence` | Validate dependency version convergence |

## Test Framework

Tests use JUnit 5 (Jupiter) with JUnit 4 (Vintage) compatibility. Parallel test execution is opt-in via `@Execution(ExecutionMode.CONCURRENT)`. Fork counts: 4 for unit tests, 2 for integration tests.
