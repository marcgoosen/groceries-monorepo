# Groceries Monorepo - Kotlin Kafka Streams

A real-time groceries recommendation engine built with Kotlin and Kafka Streams. It processes a stream of orders to find co-occurring products and generates "related products" suggestions.

## Architecture

This project is a monorepo containing:

- **apps/recommender-app**: The main application which handles:
  - **Sales Simulator**: Generates random product data and customer orders (Avro).
  - **Recommendation Pipeline**: A Kafka Streams topology that calculates co-occurrences and produces recommendations.
  - **REST API**: Ktor-based endpoints for health checks and Prometheus metrics.
- **libs/shared**: Shared utilities and domain models.

### Key Features

- **Kotlin**: Uses Kotlin in functional style.
- **Kafka Streams**: Uses advanced windowing and aggregation to find product relationships in real-time.
- **Avro Serialization**: Uses `avro4k` to autmatically create schema from data classes.
- **Hoplite Config**: Type-safe configuration via YAML and Environment Variables.
- **Observability**: Built-in Prometheus metrics and health endpoints (`/health/liveness`, `/health/readiness`, `/prometheus`).

## Prerequisites

- **JDK 21**
- **Docker & Docker Compose**

## Getting Started

### 1. Start Infrastructure

Initialize the Kafka cluster and Schema Registry:

```bash
docker compose down -v && docker compose up -d
```

You can view the Kafka UI at [http://localhost:9080](http://localhost:9080).

### 2. Build and Test

```bash
./gradlew build
```

### 3. Run the Application

The application includes an integrated simulator that starts automatically if configured.

```bash
./gradlew :apps:recommender-app:run
```

## Configuration

Configuration is managed in `apps/recommender-app/src/main/resources/application.yaml`.

Main toggles:

- `main.create-topics`: Automatically create required Kafka topics on startup.
- `main.start-simulator`: Start the background order generator.

## Data Schema

- **Products**: Static/Lookup data produced to `groceries.products.v1`.
- **Orders**: Transactional stream on `groceries.orders.v1`.
- **Related Products**: The output recommendations on `groceries.related-products.v1`.

## Still TODO
- [ ] Github Actions
- [ ] Docker Image Creation
- [ ] Kubernetes setup using Kustomize
- [ ] Extract Simulator to its own app
- [ ] Separate Kafka Initializer app, that creates shared topics and create/updates shared schema's and fails early when incompatible
