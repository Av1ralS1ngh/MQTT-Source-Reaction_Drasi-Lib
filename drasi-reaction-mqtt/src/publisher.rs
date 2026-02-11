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

use serde_json::Value;

/// Serialize a query result into a JSON payload suitable for MQTT publishing.
///
/// Wraps each result with metadata about the query and operation type.
pub fn result_to_payload(
    query_id: &str,
    sequence: u64,
    added: &[Value],
    updated: &[Value],
    removed: &[Value],
) -> serde_json::Result<Vec<u8>> {
    let payload = serde_json::json!({
        "query_id": query_id,
        "sequence": sequence,
        "added": added,
        "updated": updated,
        "removed": removed,
    });
    serde_json::to_vec(&payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result_serialization() {
        let added = vec![serde_json::json!({"name": "sensor-1", "temp": 35.0})];
        let bytes = result_to_payload("temp-alert", 1, &added, &[], &[]).unwrap();
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(parsed["query_id"], "temp-alert");
        assert_eq!(parsed["sequence"], 1);
        assert_eq!(parsed["added"][0]["name"], "sensor-1");
        assert!(parsed["updated"].as_array().unwrap().is_empty());
        assert!(parsed["removed"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_empty_result() {
        let bytes = result_to_payload("q1", 0, &[], &[], &[]).unwrap();
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(parsed["query_id"], "q1");
        assert!(parsed["added"].as_array().unwrap().is_empty());
    }
}
