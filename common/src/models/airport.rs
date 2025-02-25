use super::tracking_data::Degrees;

pub type Id = u16;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct Airport {
    pub id: Id,
    pub name: String,
    pub latitude: Degrees,
    pub longitude: Degrees,
    pub city: String,
    pub country: String,
}

impl Airport {
    pub fn unknown() -> Self {
        Self {
            id: 0,
            name: "Unknown Airport".to_string(),
            latitude: 0.0,
            longitude: 0.0,
            city: "Unknown".to_string(),
            country: "Unknown".to_string(),
        }
    }
}
