# MQTT Source & Reaction for Drasi Lib

This repository implements **MQTT Source** and **Reaction** plugins for [Drasi](https://drasi.io), enabling real-time change data capture and reaction processing over MQTT.

It is structured as a Cargo workspace containing reusable library crates and an example implementation.

## Architecture

- **`drasi-source-mqtt`**: A Source plugin that subscribes to MQTT topics, parses JSON payloads, and ingests them into Drasi as graph updates. It supports "UPSERT" semantics by tracking seen IDs to distinguish between `Create` and `Update` operations.
- **`drasi-reaction-mqtt`**: A Reaction plugin that subscribes to Drasi continuous queries and publishes the results (Added/Updated/Deleted changes) to MQTT topics.
- **`drasi-lib`**: The core library providing the plugin traits (`Source`, `Reaction`) and the runtime environment.

## Crates

| Crate                  | Type    | Description                                        |
| ---------------------- | ------- | -------------------------------------------------- |
| `drasi-source-mqtt`    | Library | Implements `drasi_lib::Source`.                    |
| `drasi-reaction-mqtt`  | Library | Implements `drasi_lib::Reaction`.                  |
| `examples/iot-gateway` | Binary  | Example application combining Source and Reaction. |

## Usage

### 1. MQTT Source configuration

```rust
use drasi_source_mqtt::{MqttSource, MqttSourceConfig};

let config = MqttSourceConfig::builder("my-source", "broker.hivemq.com", "sensors/#")
    .node_label("Sensor")    // Label for the graph nodes
    .id_field("device_id")   // JSON field to use as the Entity ID
    .build();

let source = MqttSource::new(config)?;
```

### 2. MQTT Reaction configuration

```rust
use drasi_reaction_mqtt::{MqttReaction, MqttReactionConfig};

let config = MqttReactionConfig::builder(
    "my-reaction",
    "broker.hivemq.com", 
    "alerts/temperature",
    vec!["high-temp-alert".to_string()] // Queries to subscribe to
).build();

let reaction = MqttReaction::new(config);
```

### 3. Running the Gateway

```rust
let core = DrasiLib::builder()
    .with_source(source)
    .with_reaction(reaction)
    .with_query(
        Query::cypher("high-temp-alert")
            .query("MATCH (n:Sensor) WHERE n.temp > 50 RETURN n")
            .from_source("my-source")
            .build()
    )
    .build()
    .await?;

core.start().await?;
```

## Build & Test

This project is a Cargo workspace.

```bash
# Build all crates
cargo build --workspace

# Run unit tests
cargo test --workspace

# Run the example gateway
# (Requires an MQTT broker running on localhost:1883)
cargo run -p iot-gateway
```

## Implementation Details

- **Source Mapper**: The source uses a pure function `payload_to_source_change` that extracts IDs and maps JSON fields to Element Properties. It maintains a `seen_ids` set to correctly emit `ChangeOp::Create` for new entities and `ChangeOp::Update` for existing ones.
- **Reaction Publisher**: The reaction serializes `QueryResult` objects into a standardized JSON format containing `added`, `updated`, and `removed` result sets.