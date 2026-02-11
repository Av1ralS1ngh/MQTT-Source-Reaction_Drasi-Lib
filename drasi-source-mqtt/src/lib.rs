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

//! MQTT source plugin for drasi-lib.
//!
//! Subscribes to MQTT topics, parses JSON payloads, and dispatches them as
//! [`SourceChange`] events through Drasi's event pipeline.
//!
//! # Example
//!
//! ```ignore
//! use drasi_source_mqtt::{MqttSource, MqttSourceConfig};
//!
//! let config = MqttSourceConfig::builder("mqtt-src", "broker.local", "sensors/#")
//!     .port(1883)
//!     .node_label("SensorReading")
//!     .id_field("device_id")
//!     .build();
//!
//! let source = MqttSource::new(config)?;
//! // Pass `source` to DrasiLib::builder().with_source(source)
//! ```

pub mod config;
pub mod mapper;
pub mod source;

pub use config::{MqttSourceConfig, MqttSourceConfigBuilder};
pub use source::MqttSource;
