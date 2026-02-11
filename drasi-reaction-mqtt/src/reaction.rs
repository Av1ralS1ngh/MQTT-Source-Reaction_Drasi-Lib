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

//! MQTT reaction implementation of the [`Reaction`] trait.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use handlebars::Handlebars;
use log::{error, info, warn};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde_json::Value;
use tokio::sync::RwLock;

use drasi_lib::channels::ComponentStatus;
use drasi_lib::context::ReactionRuntimeContext;
use drasi_lib::reactions::{ReactionBase, ReactionBaseParams};
use drasi_lib::Reaction;

use crate::config::MqttReactionConfig;
use crate::publisher;

/// MQTT reaction plugin for drasi-lib.
///
/// Subscribes to Drasi query results via [`ReactionBase`] and publishes each
/// result batch as a JSON payload to an MQTT topic.
/// Supports dynamic topics and payloads via Handlebars templates.
pub struct MqttReaction {
    base: ReactionBase,
    config: MqttReactionConfig,
    /// MQTT client handle (set on start, cleared on stop).
    client: Arc<RwLock<Option<AsyncClient>>>,
    /// Handlebars registry for rendering templates.
    registry: Arc<Handlebars<'static>>,
}

impl MqttReaction {
    /// Create a new MQTT reaction from the given config.
    pub fn new(config: MqttReactionConfig) -> Self {
        let params = ReactionBaseParams::new(&config.id, config.queries.clone());
        let base = ReactionBase::new(params);
        let registry = Arc::new(Handlebars::new());

        Self {
            base,
            config,
            client: Arc::new(RwLock::new(None)),
            registry,
        }
    }
}

#[async_trait]
impl Reaction for MqttReaction {
    fn id(&self) -> &str {
        &self.base.id
    }

    fn type_name(&self) -> &str {
        "mqtt"
    }

    fn properties(&self) -> HashMap<String, Value> {
        let mut props = HashMap::new();
        props.insert("broker_host".into(), Value::String(self.config.broker_host.clone()));
        props.insert("port".into(), Value::Number(self.config.port.into()));
        props.insert("topic".into(), Value::String(self.config.topic.clone()));
        props
    }

    fn query_ids(&self) -> Vec<String> {
        self.base.queries.clone()
    }

    fn auto_start(&self) -> bool {
        self.base.get_auto_start()
    }

    async fn initialize(&self, context: ReactionRuntimeContext) {
        self.base.initialize(context).await;
    }

    async fn start(&self) -> Result<()> {
        info!(
            "[{}] Starting MQTT reaction (broker={}:{}, topic={})",
            self.config.id, self.config.broker_host, self.config.port, self.config.topic
        );

        // Build MQTT options.
        let mut mqtt_opts = MqttOptions::new(
            &self.config.client_id,
            &self.config.broker_host,
            self.config.port,
        );
        mqtt_opts.set_keep_alive(std::time::Duration::from_secs(30));

        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            mqtt_opts.set_credentials(user, pass);
        }

        let (client, mut eventloop) = AsyncClient::new(mqtt_opts, 100);
        *self.client.write().await = Some(client.clone());

        // Subscribe to all configured queries.
        self.base.subscribe_to_queries().await?;

        // Clone what we need for the spawned tasks.
        let base = self.base.clone_shared();
        let topic_template = self.config.topic.clone();
        let payload_template = self.config.payload_template.clone();
        let reaction_id = self.config.id.clone();
        let registry = self.registry.clone();

        // Create shutdown channel.
        let shutdown_rx = self.base.create_shutdown_channel().await;

        // Spawn the MQTT eventloop driver (keeps connection alive).
        let eventloop_id = reaction_id.clone();
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("[{eventloop_id}] MQTT eventloop error (will reconnect): {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });

        // Spawn the main processing loop: dequeue from priority queue → publish to MQTT.
        let handle = tokio::spawn(async move {
            info!("[{reaction_id}] Processing loop started");
            let mut sequence: u64 = 0;
            let mut shutdown_rx = shutdown_rx;

            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        info!("[{reaction_id}] Shutdown signal received");
                        break;
                    }
                    result = base.priority_queue.dequeue() => {
                        sequence += 1;

                        use drasi_lib::channels::ResultDiff;
                        
                        let query_id = &result.query_id;
                        let mut added = Vec::new();
                        let mut updated = Vec::new();
                        let mut removed = Vec::new();

                        for diff in &result.results {
                            match diff {
                                ResultDiff::Add { data } => added.push(data.clone()),
                                ResultDiff::Delete { data } => removed.push(data.clone()),
                                ResultDiff::Update { after, .. } => updated.push(after.clone()),
                                _ => {}
                            }
                        }

                        match publisher::result_to_payload(
                            query_id, 
                            sequence, 
                            &added, 
                            &updated, 
                            &removed,
                            &registry,
                            &topic_template,
                            payload_template.as_deref()
                        ) {
                            Ok(messages) => {
                                for (topic, payload) in messages {
                                    if let Err(e) = client
                                        .publish(topic, QoS::AtLeastOnce, false, payload)
                                        .await
                                    {
                                        error!("[{reaction_id}] Failed to publish to MQTT: {e}");
                                    }
                                }
                            }
                            Err(e) => {
                                error!("[{reaction_id}] Failed to process result: {e}");
                            }
                        }
                    }
                }
            }
        });

        self.base.set_processing_task(handle).await;
        info!("[{}] MQTT reaction started", self.config.id);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(client) = self.client.write().await.take() {
            let _ = client.disconnect().await;
        }
        self.base.stop_common().await
    }

    async fn status(&self) -> ComponentStatus {
        self.base.get_status().await
    }
}
