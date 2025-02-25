use crate::frame::messages::notation::{
    read_string_map, read_string_multimap, write_string_map, write_string_multimap,
};
use std::io::Cursor;

const CQL_VERSION_KEY: &str = "CQL_VERSION";
const CQL_VERSION_VALUE: &str = "3.0.0";
const COMPRESSION_KEY: &str = "COMPRESSION";
const COMPRESSION_VALUE: &str = "";

pub fn deserialize_startup(body: &[u8]) -> std::io::Result<Vec<(String, String)>> {
    read_string_map(&mut Cursor::new(body))
}

pub fn deserialize_options(body: &[u8]) -> std::io::Result<Vec<(String, Vec<String>)>> {
    read_string_multimap(&mut Cursor::new(body))
}

pub fn serialize_startup(options_selected: &[(String, String)]) -> Vec<u8> {
    let mut body = Vec::new();
    let options_selected = options_selected
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    write_string_map(&mut body, options_selected);
    body
}

pub fn serialize_options(options: &[(String, Vec<String>)]) -> Vec<u8> {
    let mut body = Vec::new();
    let options: Vec<(&str, Vec<&str>)> = options
        .iter()
        .map(|(key, value)| (key.as_str(), value.iter().map(|s| s.as_str()).collect()))
        .collect();
    write_string_multimap(&mut body, options);
    body
}

pub fn default_startup() -> Vec<(String, String)> {
    vec![(CQL_VERSION_KEY.to_string(), CQL_VERSION_VALUE.to_string())]
}

pub fn default_supported() -> Vec<(String, Vec<String>)> {
    vec![
        (
            CQL_VERSION_KEY.to_string(),
            vec![CQL_VERSION_VALUE.to_string()],
        ),
        (
            COMPRESSION_KEY.to_string(),
            vec![COMPRESSION_VALUE.to_string()],
        ),
    ]
}

pub fn validate_options(options: &Vec<(String, String)>) -> bool {
    let mut explicit_version = false;
    for (key, value) in options {
        match key.as_str() {
            CQL_VERSION_KEY => {
                if value != CQL_VERSION_VALUE {
                    return false;
                }
                explicit_version = true;
            }
            COMPRESSION_KEY => {
                if value.is_empty() {
                    return false;
                }
            }
            _ => return false,
        }
    }
    explicit_version
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_startup() {
        let startup_options = default_startup();
        assert_eq!(startup_options.len(), 1);
        assert_eq!(startup_options[0].0, CQL_VERSION_KEY);
        assert_eq!(startup_options[0].1, CQL_VERSION_VALUE);
    }

    #[test]
    fn test_default_supported() {
        let supported_options = default_supported();
        assert_eq!(supported_options.len(), 2);
        assert_eq!(supported_options[0].0, CQL_VERSION_KEY);
        assert_eq!(supported_options[0].1, vec![CQL_VERSION_VALUE]);
        assert_eq!(supported_options[1].0, COMPRESSION_KEY);
        assert_eq!(supported_options[1].1, vec![COMPRESSION_VALUE]);
    }

    #[test]
    fn test_validate_options_invalid_version() {
        let invalid_options = vec![
            (CQL_VERSION_KEY.to_string(), "2.0.0".to_string()), // Invalid version
            (COMPRESSION_KEY.to_string(), COMPRESSION_VALUE.to_string()),
        ];
        assert!(!validate_options(&invalid_options));
    }

    #[test]
    fn test_validate_options_missing_version() {
        let missing_version_options = vec![
            (COMPRESSION_KEY.to_string(), COMPRESSION_VALUE.to_string()), // Missing version
        ];
        assert!(!validate_options(&missing_version_options));
    }

    #[test]
    fn test_serialize_startup() {
        let options = vec![
            (CQL_VERSION_KEY.to_string(), CQL_VERSION_VALUE.to_string()),
            (COMPRESSION_KEY.to_string(), COMPRESSION_VALUE.to_string()),
        ];
        let serialized = serialize_startup(&options);

        // Deserialize to check it was serialized correctly
        let deserialized = deserialize_startup(&serialized).unwrap();
        assert_eq!(deserialized, options);
    }

    #[test]
    fn test_serialize_options() {
        let options = vec![
            (
                CQL_VERSION_KEY.to_string(),
                vec![CQL_VERSION_VALUE.to_string()],
            ),
            (
                COMPRESSION_KEY.to_string(),
                vec![COMPRESSION_VALUE.to_string()],
            ),
        ];
        let serialized = serialize_options(&options);

        // Deserialize to check it was serialized correctly
        let deserialized = deserialize_options(&serialized).unwrap();
        assert_eq!(deserialized, options);
    }
}
