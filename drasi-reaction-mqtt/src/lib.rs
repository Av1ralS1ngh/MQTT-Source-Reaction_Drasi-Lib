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

//! MQTT reaction plugin for drasi-lib.
//!
//! Subscribes to Drasi query results and publishes them as JSON payloads
//! to an MQTT broker.
//!
//! # Example
//!
//! ```ignore
//! use drasi_reaction_mqtt::{MqttReaction, MqttReactionConfig};
//!
//! let config = MqttReactionConfig::builder(
//!     "mqtt-alert",
//!     "broker.local",
//!     "alerts/drasi",
//!     vec!["temp-alert".into()],
//! )
//! .port(1883)
//! .build();
//!
//! let reaction = MqttReaction::new(config);
//! // Pass `reaction` to DrasiLib::builder().with_reaction(reaction)
//! ```

pub mod config;
pub mod publisher;
pub mod reaction;

pub use config::{MqttReactionConfig, MqttReactionConfigBuilder};
pub use reaction::MqttReaction;
