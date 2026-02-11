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

//! Payload mapping utilities for converting MQTT JSON payloads to [`SourceChange`].

//! Payload mapping utilities for converting MQTT JSON payloads to [`SourceChange`].
 
 use drasi_core::models::{ElementMetadata, ElementPropertyMap, ElementReference, SourceChange};
 use serde_json::Value;
 use std::sync::Arc;
 
 use crate::config::OperationMode;
 
 /// Converts a raw JSON payload into a [`SourceChange`].
 ///
 /// Uses `operation_mode` to determine whether to emit Insert or Update.
 ///
 /// # Arguments
 /// * `payload` - Raw JSON bytes from MQTT.
 /// * `id_field` - Name of the JSON field to use as entity ID.
 /// * `node_label` - Graph node label (e.g. `"SensorReading"`).
 /// * `mode` - Operation mode (Insert or Update).
 pub fn payload_to_source_change(
     payload: &[u8],
     id_field: &str,
     node_label: &str,
     mode: OperationMode,
 ) -> Result<SourceChange, serde_json::Error> {
     let json: Value = serde_json::from_slice(payload)?;
 
     // Extract entity ID from the configured field, or generate a UUID.
     let entity_id = json
         .get(id_field)
         .and_then(|v| match v {
             Value::String(s) => Some(s.clone()),
             Value::Number(n) => Some(n.to_string()),
             _ => None,
         })
         .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
 
     // Build property map
     let mut properties = ElementPropertyMap::new();
     if let Value::Object(map) = &json {
         for (key, value) in map {
             properties.insert(key.as_str(), value.into());
         }
     }
 
     let metadata = ElementMetadata {
         reference: ElementReference::new(node_label, &entity_id),
         labels: vec![Arc::from(node_label)].into(),
         effective_from: 0,
     };
 
     let element = drasi_core::models::Element::Node {
         metadata,
         properties,
     };
 
     let change = match mode {
         OperationMode::Insert => SourceChange::Insert { element },
         OperationMode::Update => SourceChange::Update { element },
     };
 
     Ok(change)
 }
 
 #[cfg(test)]
 mod tests {
     use super::*;
 
     #[test]
     fn test_insert_mode() {
         let payload = br#"{"id": "sensor-1", "temp": 25.5}"#;
         let change = payload_to_source_change(payload, "id", "Sensor", OperationMode::Insert).unwrap();
 
         match change {
             SourceChange::Insert { element } => {
                 assert_eq!(element.get_reference().element_id.as_ref(), "sensor-1");
             }
             _ => panic!("Expected Insert"),
         }
     }
 
     #[test]
     fn test_update_mode() {
         let payload = br#"{"id": "sensor-1", "temp": 30.0}"#;
         let change = payload_to_source_change(payload, "id", "Sensor", OperationMode::Update).unwrap();
 
         match change {
             SourceChange::Update { element } => {
                 assert_eq!(element.get_reference().element_id.as_ref(), "sensor-1");
             }
             _ => panic!("Expected Update"),
         }
     }
 
     #[test]
     fn test_uuid_fallback_when_id_missing() {
         let payload = br#"{"temp": 25.5}"#;
         let change = payload_to_source_change(payload, "id", "Sensor", OperationMode::Insert).unwrap();
 
         match change {
             SourceChange::Insert { element } => {
                 assert!(!element.get_reference().element_id.is_empty());
             }
             _ => panic!("Expected Insert"),
         }
     }
 
     #[test]
     fn test_numeric_id_field() {
         let payload = br#"{"device_id": 42, "temp": 20.0}"#;
         let change = payload_to_source_change(payload, "device_id", "Sensor", OperationMode::Insert).unwrap();
 
         assert_eq!(change.get_reference().element_id.as_ref(), "42");
     }
 
     #[test]
     fn test_invalid_json() {
         let payload = b"not json";
         assert!(payload_to_source_change(payload, "id", "Sensor", OperationMode::Insert).is_err());
     }
 }
