pub type Id = u16;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct Airplane {
    pub id: Id,
    pub model: String,
    pub max_fuel: super::tracking_data::Liters,
}
