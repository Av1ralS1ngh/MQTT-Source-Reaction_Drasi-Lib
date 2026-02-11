# MQTT Source & Reaction for Drasi

This repository provides **MQTT Source** and **Reaction** plugins for [Drasi](https://drasi.io), enabling real-time change data capture and reaction processing over MQTT.

## Components

### 1. MQTT Source (`drasi-source-mqtt`)
Ingests JSON payloads from an MQTT topic into Drasi as Graph element updates.
*   **Stateless Operation**: Configurable `OperationMode` controls how messages are treated.
    *   **Insert**: Treats every message as a new entity (default).
    *   **Update**: Treats every message as an update to an existing entity.
*   **ID Mapping**: Extracts a specified JSON field to use as the Entity ID.

### 2. MQTT Reaction (`drasi-reaction-mqtt`)
Publishes Drasi query results to MQTT topics.
*   **Dynamic Topics**: Supports Handlebars templates (e.g., `devices/{{device_id}}/alert`).
*   **Flexible Payloads**:
    *   **Templated**: Render custom JSON payloads for each result item using Handlebars.
    *   **Batched**: Efficiently publish all results in a single standard JSON batch if no template is used.

## Usage Examples

### MQTT Source Configuration
```rust
use drasi_source_mqtt::{MqttSource, MqttSourceConfig, OperationMode};

let config = MqttSourceConfig::builder("sensor-source", "broker.hivemq.com", "sensors/#")
    .node_label("Sensor")    // Graph node label
    .id_field("device_id")   // JSON field for ID
    .mode(OperationMode::Update) // Treat messages as updates
    .build();

let source = MqttSource::new(config)?;
```

### MQTT Reaction Configuration
```rust
use drasi_reaction_mqtt::{MqttReaction, MqttReactionConfig};

let config = MqttReactionConfig::builder(
    "alert-reaction",
    "broker.hivemq.com", 
    "devices/{{device_id}}/commands", // Dynamic topic
    vec!["high-temp-alert".to_string()]
)
.payload_template(r#"{"command": "shutdown", "reason": "{{temp}}"}"#) // Custom payload
.build();

let reaction = MqttReaction::new(config);
```

## Build & Test

This project is a standard Cargo workspace.

```bash
# Build all crates
cargo build --workspace

# Run unit tests
cargo test --workspace

# Run the example gateway (requires local MQTT broker)
cargo run -p iot-gateway
```