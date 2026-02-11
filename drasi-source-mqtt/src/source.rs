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

//! MQTT source implementation of the [`Source`] trait.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashSet;
use log::{error, info, warn};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde_json::Value;
use tokio::sync::RwLock;

use drasi_lib::channels::{ComponentStatus, DispatchMode, SubscriptionResponse};
use drasi_lib::context::SourceRuntimeContext;
use drasi_lib::sources::base::{SourceBase, SourceBaseParams};
use drasi_lib::Source;

use crate::config::MqttSourceConfig;
use crate::mapper;

/// MQTT source plugin for drasi-lib.
///
/// Subscribes to an MQTT broker topic, parses incoming JSON payloads into
/// graph-node `SourceChange` events, and dispatches them through the Drasi
/// pipeline via [`SourceBase`].
pub struct MqttSource {
    base: SourceBase,
    config: MqttSourceConfig,
    /// Track seen entity IDs for Create vs Update semantics.
    seen_ids: Arc<DashSet<String>>,
    /// MQTT client handle (set on start, cleared on stop).
    client: Arc<RwLock<Option<AsyncClient>>>,
}

impl MqttSource {
    /// Create a new MQTT source from the given config.
    pub fn new(config: MqttSourceConfig) -> Result<Self> {
        let params = SourceBaseParams::new(&config.id);
        let base = SourceBase::new(params)?;

        Ok(Self {
            base,
            config,
            seen_ids: Arc::new(DashSet::new()),
            client: Arc::new(RwLock::new(None)),
        })
    }
}

#[async_trait]
impl Source for MqttSource {
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
        props.insert("node_label".into(), Value::String(self.config.node_label.clone()));
        props.insert("id_field".into(), Value::String(self.config.id_field.clone()));
        props
    }

    fn dispatch_mode(&self) -> DispatchMode {
        DispatchMode::Channel
    }

    fn auto_start(&self) -> bool {
        self.base.auto_start
    }

    async fn initialize(&self, context: SourceRuntimeContext) {
        self.base.initialize(context).await;
    }

    async fn start(&self) -> Result<()> {
        info!(
            "[{}] Starting MQTT source (broker={}:{}, topic={})",
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

        // Subscribe to the configured topic.
        client
            .subscribe(&self.config.topic, QoS::AtLeastOnce)
            .await
            .map_err(|e| anyhow::anyhow!("MQTT subscribe failed: {e}"))?;

        // Store client for later disconnect.
        *self.client.write().await = Some(client);

        // Clone what we need for the spawned task.
        let base = self.base.clone_shared();
        let id_field = self.config.id_field.clone();
        let node_label = self.config.node_label.clone();
        let seen_ids = self.seen_ids.clone();
        let source_id = self.config.id.clone();

        // Create shutdown channel.
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        self.base.set_shutdown_tx(shutdown_tx).await;

        // Spawn the MQTT event loop task.
        let handle = tokio::spawn(async move {
            info!("[{source_id}] MQTT event loop started");
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        info!("[{source_id}] Shutdown signal received");
                        break;
                    }
                    event = eventloop.poll() => {
                        match event {
                            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                                match mapper::payload_to_source_change(
                                    &publish.payload,
                                    &id_field,
                                    &node_label,
                                    &seen_ids,
                                ) {
                                    Ok(change) => {
                                        if let Err(e) = base.dispatch_source_change(change).await {
                                            error!("[{source_id}] Failed to dispatch change: {e}");
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "[{source_id}] Failed to parse payload on topic '{}': {e}",
                                            publish.topic
                                        );
                                    }
                                }
                            }
                            Ok(_) => {} // Ignore other events (ConnAck, PingResp, etc.)
                            Err(e) => {
                                error!("[{source_id}] MQTT connection error: {e}");
                                // rumqttc will auto-reconnect on next poll()
                            }
                        }
                    }
                }
            }
        });

        self.base.set_task_handle(handle).await;
        self.base.set_status(ComponentStatus::Running).await;
        info!("[{}] MQTT source started", self.config.id);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        // Disconnect the MQTT client.
        if let Some(client) = self.client.write().await.take() {
            let _ = client.disconnect().await;
        }
        self.base.stop_common().await
    }

    async fn status(&self) -> ComponentStatus {
        self.base.get_status().await
    }

    async fn subscribe(
        &self,
        settings: drasi_lib::SourceSubscriptionSettings,
    ) -> Result<SubscriptionResponse> {
        self.base
            .subscribe_with_bootstrap(&settings, "mqtt")
            .await
    }

    async fn set_bootstrap_provider(
        &self,
        provider: Box<dyn drasi_lib::BootstrapProvider + 'static>,
    ) {
        self.base.set_bootstrap_provider(provider).await;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
