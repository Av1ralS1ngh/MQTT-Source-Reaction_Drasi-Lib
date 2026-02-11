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

//! Utility functions for serializing query results to MQTT payloads.

use handlebars::Handlebars;
use serde_json::Value;

/// Serialize a query result into a list of (topic, payload) pairs.
///
/// * `topic_template`: The MQTT topic (can be a Handlebars template).
/// * `payload_template`: Optional Handlebars template for the payload.
///
/// Logic:
/// 1. If `topic_template` contains "{{" OR `payload_template` is Some, we split the batch.
///    For each item in added/updated/removed, we render the topic and payload.
/// 2. Otherwise, we publish a single batched message to the static topic.
pub fn result_to_payload(
    query_id: &str,
    sequence: u64,
    added: &[Value],
    updated: &[Value],
    removed: &[Value],
    registry: &Handlebars,
    topic_template: &str,
    payload_template: Option<&str>,
) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
    let mut messages = Vec::new();

    let split_mode = topic_template.contains("{{") || payload_template.is_some();

    if split_mode {
        // Helper to process a list
        let mut process_list = |list: &[Value], op: &str| -> anyhow::Result<()> {
            for item in list {
                // Prepare context
                let mut context = serde_json::to_value(item)?;
                if let Value::Object(ref mut map) = context {
                    map.insert("query_id".to_string(), query_id.into());
                    map.insert("sequence".to_string(), sequence.into());
                    map.insert("op".to_string(), op.into());
                }

                // Render Topic
                let topic = registry.render_template(topic_template, &context)?;

                // Render Payload
                let payload = if let Some(tmpl) = payload_template {
                    registry.render_template(tmpl, &context)?.into_bytes()
                } else {
                    // If no payload template but we are splitting (due to dynamic topic),
                    // we serialize the single item + metadata as JSON.
                    serde_json::to_vec(&context)?
                };

                messages.push((topic, payload));
            }
            Ok(())
        };

        process_list(added, "insert")?;
        process_list(updated, "update")?;
        process_list(removed, "delete")?;
    } else {
        // Batch mode: Static topic, default massive JSON payload
        let payload = serde_json::json!({
            "query_id": query_id,
            "sequence": sequence,
            "added": added,
            "updated": updated,
            "removed": removed,
        });
        let bytes = serde_json::to_vec(&payload)?;
        messages.push((topic_template.to_string(), bytes));
    }

    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_mode() {
        let registry = Handlebars::new();
        let added = vec![serde_json::json!({"name": "sensor-1", "temp": 35.0})];
        let messages = result_to_payload(
            "q1", 1, &added, &[], &[], 
            &registry, "static/topic", None
        ).unwrap();
        
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, "static/topic");
        
        let parsed: Value = serde_json::from_slice(&messages[0].1).unwrap();
        assert_eq!(parsed["query_id"], "q1");
        assert_eq!(parsed["added"][0]["name"], "sensor-1");
    }

    #[test]
    fn test_split_mode_dynamic_topic() {
        let registry = Handlebars::new();
        let added = vec![
            serde_json::json!({"device": "d1", "val": 1}),
            serde_json::json!({"device": "d2", "val": 2}),
        ];
        
        let messages = result_to_payload(
            "q1", 1, &added, &[], &[], 
            &registry, "devices/{{device}}/data", None
        ).unwrap();

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].0, "devices/d1/data");
        assert_eq!(messages[1].0, "devices/d2/data");
    }

    #[test]
    fn test_split_mode_payload_template() {
        let registry = Handlebars::new();
        let added = vec![serde_json::json!({"device": "d1"})];
        
        let messages = result_to_payload(
            "q1", 1, &added, &[], &[], 
            &registry, "static/topic", Some("Alert: {{device}}")
        ).unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, "static/topic");
        assert_eq!(String::from_utf8(messages[0].1.clone()).unwrap(), "Alert: d1");
    }
}
