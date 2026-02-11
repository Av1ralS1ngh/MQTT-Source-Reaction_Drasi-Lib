// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::Result;
use drasi_lib::{DrasiLib, Query};
use drasi_reaction_mqtt::{MqttReaction, MqttReactionConfig};
use drasi_source_mqtt::{MqttSource, MqttSourceConfig};
use log::info;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    info!("Starting IoT Gateway Drasi Example...");

    // 1. Configure the MQTT Source
    // Subscribes to 'sensors/#' and maps JSON payloads to 'SensorReading' nodes
    let source_config = MqttSourceConfig::builder("mqtt-src", "localhost", "sensors/#")
        .port(1883)
        .client_id("drasi-iot-gateway-source")
        .node_label("SensorReading")
        .id_field("device_id")
        .build();

    let source = MqttSource::new(source_config)?;

    // 2. Configure the MQTT Reaction
    // Publishes results of 'high-temp-alert' to 'alerts/high-temp'
    let reaction_config = MqttReactionConfig::builder(
        "mqtt-alert",
        "localhost",
        "alerts/high-temp",
        vec!["high-temp-alert".to_string()],
    )
    .port(1883)
    .client_id("drasi-iot-gateway-reaction")
    .build();

    let reaction = MqttReaction::new(reaction_config);

    // 3. Define the Continuous Query
    // Detects SensorReadings where temperature > 30 and no existing alert is active
    // Note: The graph logic depends on your data model. This is a simple filters.
    let query = Query::cypher("high-temp-alert")
        .query("MATCH (s:SensorReading) WHERE s.temperature > 30 RETURN s")
        .from_source("mqtt-src")
        .build();

    // 4. Build and Start DrasiLib
    let core = DrasiLib::builder()
        .with_id("iot-gateway-core")
        .with_source(source)
        .with_reaction(reaction)
        .with_query(query)
        .build()
        .await?;

    info!("DrasiLib configured successfully. Starting...");
    core.start().await?;

    // Keep running until signal
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received");

    // Clean stop handled by DrasiLib drop/shutdown logic
    Ok(())
}
