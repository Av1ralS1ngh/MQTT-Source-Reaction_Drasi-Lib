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

//! Configuration types for the MQTT reaction plugin.

use serde::Deserialize;

/// Configuration for the MQTT reaction.
#[derive(Debug, Clone, Deserialize)]
pub struct MqttReactionConfig {
    /// Unique reaction identifier.
    pub id: String,
    /// MQTT broker hostname or IP.
    pub broker_host: String,
    /// MQTT broker port (default: 1883).
    pub port: u16,
    /// MQTT topic template to publish results to (e.g. `devices/{{device_id}}/commands`).
    pub topic: String,
    /// Optional payload template (Handlebars). If not provided, default JSON serialization is used.
    pub payload_template: Option<String>,
    /// MQTT client ID. Defaults to `"drasi-reaction-{id}"`.
    pub client_id: String,
    /// Optional MQTT username for authentication.
    pub username: Option<String>,
    /// Optional MQTT password for authentication.
    pub password: Option<String>,
    /// List of query IDs this reaction subscribes to.
    pub queries: Vec<String>,
}

impl MqttReactionConfig {
    /// Start building a new config with required fields.
    pub fn builder(
        id: impl Into<String>,
        broker_host: impl Into<String>,
        topic: impl Into<String>,
        queries: Vec<String>,
    ) -> MqttReactionConfigBuilder {
        let id = id.into();
        MqttReactionConfigBuilder {
            id: id.clone(),
            broker_host: broker_host.into(),
            topic: topic.into(),
            payload_template: None,
            port: 1883,
            client_id: format!("drasi-reaction-{id}"),
            username: None,
            password: None,
            queries,
        }
    }
}

/// Builder for [`MqttReactionConfig`].
pub struct MqttReactionConfigBuilder {
    id: String,
    broker_host: String,
    topic: String,
    payload_template: Option<String>,
    port: u16,
    client_id: String,
    username: Option<String>,
    password: Option<String>,
    queries: Vec<String>,
}

impl MqttReactionConfigBuilder {
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn payload_template(mut self, template: impl Into<String>) -> Self {
        self.payload_template = Some(template.into());
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

    /// Build the config.
    pub fn build(self) -> MqttReactionConfig {
        MqttReactionConfig {
            id: self.id,
            broker_host: self.broker_host,
            port: self.port,
            topic: self.topic,
            payload_template: self.payload_template,
            client_id: self.client_id,
            username: self.username,
            password: self.password,
            queries: self.queries,
        }
    }
}
