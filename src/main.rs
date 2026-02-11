use std::{
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use drasi_source_sdk::{
    stream, ChangeOp, ChangeStream, ReactivatorBuilder, ReactivatorError, SourceChange,
    SourceElement, StateStore,
};
use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Incoming};
use serde::Deserialize;
use serde_json::{Value, Map};
use log::{info, error, warn};
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
struct MqttConfig {
    broker_url: String,
    port: u16,
    topic: String,
    client_id: String,
    username: Option<String>,
    password: Option<String>,
}

impl MqttConfig {
    fn from_env() -> Result<Self, String> {
        let broker_url = env::var("MQTT_BROKER_URL").map_err(|_| "MQTT_BROKER_URL not set")?;
        let port = env::var("MQTT_PORT")
            .unwrap_or_else(|_| "1883".to_string())
            .parse::<u16>()
            .map_err(|_| "Invalid MQTT_PORT")?;
        let topic = env::var("MQTT_TOPIC").map_err(|_| "MQTT_TOPIC not set")?;
        let client_id = env::var("MQTT_CLIENT_ID").unwrap_or_else(|_| format!("drasi-mqtt-source-{}", uuid::Uuid::new_v4()));
        let username = env::var("MQTT_USERNAME").ok();
        let password = env::var("MQTT_PASSWORD").ok();

        Ok(MqttConfig {
            broker_url,
            port,
            topic,
            client_id,
            username,
            password,
        })
    }
}

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let reactivator = ReactivatorBuilder::new()
        .with_stream_producer(mqtt_stream)
        .with_deprovision_handler(deprovision)
        .without_context()
        .build()
        .await;

    reactivator.start().await;
}

async fn deprovision(_state_store: Arc<dyn StateStore + Send + Sync>) {
    info!("Deprovisioning MQTT source (no-op)");
}

async fn mqtt_stream(
    _context: (),
    _state_store: Arc<dyn StateStore + Send + Sync>,
) -> Result<ChangeStream, ReactivatorError> {
    let config = match MqttConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            error!("Configuration error: {}", e);
            return Err(ReactivatorError::InternalError(format!("Config error: {}", e)));
        }
    };

    info!("Connecting to MQTT broker at {}:{}", config.broker_url, config.port);

    let mut mqttoptions = MqttOptions::new(&config.client_id, &config.broker_url, config.port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    if let (Some(u), Some(p)) = (&config.username, &config.password) {
        mqttoptions.set_credentials(u, p);
    }

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    
    client.subscribe(&config.topic, QoS::AtLeastOnce).await.map_err(|e| {
        error!("Failed to subscribe: {}", e);
        ReactivatorError::InternalError(format!("Subscribe error: {}", e))
    })?;

    info!("Subscribed to topic: {}", config.topic);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<SourceChange>(100);

    // Spawn the event loop handler
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(notification) => {
                    if let Event::Incoming(Incoming::Publish(publish)) = notification {
                        let payload = publish.payload;
                        let topic = publish.topic;
                        
                        let ts = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or(Duration::from_secs(0))
                            .as_nanos(); 

                        if let Some(change) = payload_to_change(&payload, ts) {
                            if let Err(e) = tx.send(change).await {
                                error!("Failed to send change to channel: {}", e);
                                break; 
                            }
                        } else {
                            warn!("Skipping invalid message from topic: {}", topic);
                        }
                    }
                }
                Err(e) => {
                    error!("MQTT Connection Error: {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });


    let result_stream = stream! {
        while let Some(change) = rx.recv().await {
            yield change;
        }
    };

    Ok(Box::pin(result_stream))
}

fn payload_to_change(payload: &[u8], ts: u128) -> Option<SourceChange> {
    let json_vals: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            warn!("Failed to parse JSON payload: {}", e);
            return None;
        }
    };
    
    let id = match json_vals.get("id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            uuid::Uuid::new_v4().to_string()
        }
    };

    let labels = vec!["MqttMessage".to_string()]; 
    
    let properties = match json_vals {
        Value::Object(map) => map,
        _ => Map::new(), 
    };

    let node = SourceElement::Node {
        id,
        labels,
        properties,
    };

    Some(SourceChange::new(ChangeOp::Create, node, ts, ts, 0, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_payload_to_change_valid_json() {
        let payload = json!({
            "id": "test-id",
            "temp": 25.5,
            "status": "active"
        }).to_string();
        
        let ts = 1234567890;
        let change = payload_to_change(payload.as_bytes(), ts).unwrap();

        // Serialize to Value to inspect private fields
        let change_val = serde_json::to_value(&change).expect("Failed to serialize");
        
        assert_eq!(change_val["op"], "i"); // ChangeOp::Create -> "i"
        assert_eq!(change_val["payload"]["after"]["id"], "test-id");
        assert_eq!(change_val["payload"]["after"]["labels"][0], "MqttMessage");
        assert_eq!(change_val["payload"]["after"]["properties"]["temp"], 25.5);
        assert_eq!(change_val["payload"]["after"]["properties"]["status"], "active");
    }

    #[test]
    fn test_payload_to_change_invalid_json() {
        let payload = "invalid-json";
        let ts = 1234567890;
        assert!(payload_to_change(payload.as_bytes(), ts).is_none());
    }

    #[test]
    fn test_payload_to_change_missing_id_generates_uuid() {
        let payload = json!({
            "temp": 25.5
        }).to_string();
        
        let ts = 1234567890;
        let change = payload_to_change(payload.as_bytes(), ts).unwrap();
        
        let change_val = serde_json::to_value(&change).expect("Failed to serialize");
        assert!(change_val["payload"]["after"]["id"].is_string());
        assert!(!change_val["payload"]["after"]["id"].as_str().unwrap().is_empty());
    }
}
