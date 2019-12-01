use serde_json::{from_str, Value};

// TODO: move validation schemas to file for flexible management
lazy_static! {
    pub static ref VALIDATION_SCHEMA_CREATE: Value = from_str(
        r#"
        {
            "type": "object",
            "properties": {
                "id": {
                    "type": "integer"
                }
            },
            "required": ["id"],
            "additionalProperties": false
        }"#,
    )
    .unwrap();
}
