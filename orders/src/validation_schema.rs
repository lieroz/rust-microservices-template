use serde_json::{from_str, Value};

// TODO: move validation schemas to file for flexible management
lazy_static! {
    pub static ref VALIDATION_SCHEMA_CREATE: Value = from_str(
        r#"
        {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "number"
                },
                "goods": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "good_id": {
                                "type": "number"
                            },
                            "price": {
                                "type": "number"
                            },
                            "name": {
                                "type": "string"
                            },
                            "description": {
                                "type": "string"
                            }
                        },
                        "required": ["good_id", "price", "name", "description"]
                    }
                }
            },
            "required": ["order_id", "goods"]
        }"#,
    )
    .unwrap();
}

lazy_static! {
    pub static ref VALIDATION_SCHEMA_UPDATE: Value = from_str(
        r#"
        {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "number"
                },
                "goods": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "good_id": {
                                "type": "number"
                            },
                            "price": {
                                "type": "number"
                            },
                            "name": {
                                "type": "string"
                            },
                            "description": {
                                "type": "string"
                            }
                        },
                        "required": ["good_id"]
                    }
                }
            },
            "required": ["order_id", "goods"]
        }"#,
    )
    .unwrap();
}
