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

//! Configuration types for the MQTT source plugin.

use serde::Deserialize;

/// Configuration for the MQTT source.
#[derive(Debug, Clone, Deserialize)]
pub struct MqttSourceConfig {
    /// Unique source identifier (used by queries to reference this source).
    pub id: String,
    /// MQTT broker hostname or IP.
    pub broker_host: String,
    /// MQTT broker port (default: 1883).
    pub port: u16,
    /// MQTT topic filter to subscribe to (supports wildcards like `sensors/#`).
    pub topic: String,
    /// MQTT client ID. Defaults to `"drasi-source-{id}"`.
    pub client_id: String,
    /// Optional MQTT username for authentication.
    pub username: Option<String>,
    /// Optional MQTT password for authentication.
    pub password: Option<String>,
    /// Label applied to graph nodes produced by this source (default: `"MqttMessage"`).
    pub node_label: String,
    /// JSON field name used as the entity ID (default: `"id"`).
    /// If the field is missing from a payload, a UUID is generated.
    pub id_field: String,
}

impl MqttSourceConfig {
    /// Start building a new config with the required fields.
    pub fn builder(
        id: impl Into<String>,
        broker_host: impl Into<String>,
        topic: impl Into<String>,
    ) -> MqttSourceConfigBuilder {
        let id = id.into();
        MqttSourceConfigBuilder {
            id: id.clone(),
            broker_host: broker_host.into(),
            topic: topic.into(),
            port: 1883,
            client_id: format!("drasi-source-{id}"),
            username: None,
            password: None,
            node_label: "MqttMessage".to_string(),
            id_field: "id".to_string(),
        }
    }
}

/// Builder for [`MqttSourceConfig`].
pub struct MqttSourceConfigBuilder {
    id: String,
    broker_host: String,
    topic: String,
    port: u16,
    client_id: String,
    username: Option<String>,
    password: Option<String>,
    node_label: String,
    id_field: String,
}

impl MqttSourceConfigBuilder {
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = client_id.into();
        self
    }

    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn node_label(mut self, label: impl Into<String>) -> Self {
        self.node_label = label.into();
        self
    }

    pub fn id_field(mut self, field: impl Into<String>) -> Self {
        self.id_field = field.into();
        self
    }

    /// Build the config.
    pub fn build(self) -> MqttSourceConfig {
        MqttSourceConfig {
            id: self.id,
            broker_host: self.broker_host,
            port: self.port,
            topic: self.topic,
            client_id: self.client_id,
            username: self.username,
            password: self.password,
            node_label: self.node_label,
            id_field: self.id_field,
        }
    }
}
