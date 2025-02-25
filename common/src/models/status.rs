use chrono::{DateTime, Duration, Utc};
use rand::{rng, Rng};

use super::FlightId;

const STATUS_VARIANTS: usize = 6;

#[derive(Debug, Clone)]
pub enum Status {
    Cancelled,
    Delayed,
    Scheduled { departing_time: DateTime<Utc> },
    Boarding { departing_time: DateTime<Utc> },
    Landed { arrived_at: DateTime<Utc> },
    OnAir,
    Unknown,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Cancelled => write!(f, "Cancelled"),
            Status::Delayed => write!(f, "Delayed"),
            Status::Scheduled { .. } => write!(f, "Scheduled"),
            Status::Boarding { .. } => write!(f, "Boarding"),
            Status::Landed { .. } => write!(f, "Landed"),
            Status::OnAir => write!(f, "On Air"),
            Status::Unknown => write!(f, "Unknown"),
        }
    }
}

impl Status {
    pub fn generate_query(&self, flight_id: FlightId) -> String {
        format!(
            "INSERT INTO status (flight_id, status) \
                VALUES ({}, '{}');",
            flight_id, self
        )
    }

    pub fn random_init() -> Self {
        let mut rng = rng();

        // Calculate percentage ranges based on the number of variants
        let cancelled_range = 0..(100 / STATUS_VARIANTS); // E.g., 20% if there are 5 variants
        let delayed_range = cancelled_range.end..(cancelled_range.end + 100 / STATUS_VARIANTS);
        let scheduled_range = delayed_range.end..(delayed_range.end + 100 / STATUS_VARIANTS);
        let boarding_range = delayed_range.end..(delayed_range.end + 100 / STATUS_VARIANTS);
        let landed_range = boarding_range.end..(boarding_range.end + 100 / STATUS_VARIANTS);
        // let on_air_range = landed_range.end..100; // The remaining percentage for OnAir

        // Generate a random value between 0 and 99
        let random_value: usize = rng.random_range(0..100);

        // Match the random value to determine the status
        match random_value {
            x if cancelled_range.contains(&x) => Status::Cancelled,
            x if delayed_range.contains(&x) => Status::Delayed,
            x if scheduled_range.contains(&x) => Status::Scheduled {
                departing_time: Utc::now() + Duration::minutes(rng.random_range(30..=12000)),
            },
            x if boarding_range.contains(&x) => Status::Boarding {
                departing_time: Utc::now() + Duration::minutes(rng.random_range(0..=30)),
            },
            x if landed_range.contains(&x) => Status::Landed {
                arrived_at: Utc::now() - Duration::minutes(rng.random_range(0..=60)),
            },
            _ => Status::OnAir,
        }
    }
}
