use chrono::{DateTime, Utc};
use rand::{rngs::ThreadRng, Rng as _};

use super::{FlightId, status::Status};

pub mod mode;
use mode::Mode;

pub type Meters = u16;
pub type Liters = u32;
pub type KmH = u16;
pub type Degrees = f32;

const MAX_ALTITUDE: Meters = 12_000;
const MIN_CRUISING: Meters = 9_000;
const SEA_LEVEL: Meters = 0;

#[derive(Debug, Clone)]
pub struct TrackingData {
    pub last_update: DateTime<Utc>,
    pub fuel_remaining: Liters,
    pub latitude: Degrees,
    pub longitude: Degrees,
    pub heading: Degrees,
    pub altitude: Meters,
    pub speed: KmH,
    pub current_mode: Mode,
}

impl TrackingData {
    pub fn empty() -> Self {
        Self {
            last_update: Utc::now(),
            fuel_remaining: 0,
            latitude: 0.0,
            longitude: 0.0,
            heading: 0.0,
            altitude: 0,
            speed: 0,
            current_mode: Mode::OnGround,
        }
    }

    pub fn generate_query(&self, flight_id: FlightId, max_fuel: Liters) -> String {
        format!(
            "INSERT INTO status 
            (flight_id, fuel, latitude, longitude, heading, altitude, speed, mode) \
            VALUES ({}, '{}/{}', '{}', '{}', '{}', '{}', '{}', '{}');",
            flight_id,
            self.fuel_remaining,
            max_fuel,
            self.latitude,
            self.longitude,
            self.heading,
            self.altitude,
            self.speed,
            self.current_mode
        )
    }

    pub fn random_init(
        status: &Status,
        max_fuel: Liters,
        origin_lat: Degrees,
        origin_lon: Degrees,
        destination_lat: Degrees,
        destination_lon: Degrees,
    ) -> Self {
        match status {
            Status::OnAir => Self::random_on_air(
                max_fuel,
                origin_lat,
                origin_lon,
                destination_lat,
                destination_lon,
            ),
            Status::Landed { .. } => Self {
                last_update: Utc::now(),
                fuel_remaining: (max_fuel * rand::rng().random_range(10..=40)) / 100,
                latitude: destination_lat,
                longitude: origin_lat,
                heading: calculate_heading(
                    origin_lat,
                    origin_lon,
                    destination_lat,
                    destination_lon,
                ),
                altitude: SEA_LEVEL,
                speed: 0,
                current_mode: Mode::OnGround,
            },
            _ => Self {
                last_update: Utc::now(),
                fuel_remaining: max_fuel,
                latitude: origin_lat,
                longitude: origin_lon,
                heading: calculate_heading(
                    origin_lat,
                    origin_lon,
                    destination_lat,
                    destination_lon,
                ),
                altitude: SEA_LEVEL,
                speed: 0,
                current_mode: Mode::OnGround,
            },
        }
    }

    pub fn random_on_air(
        max_fuel: Liters,
        origin_lat: Degrees,
        origin_lon: Degrees,
        destination_lat: Degrees,
        destination_lon: Degrees,
    ) -> Self {
        let mut rng = rand::rng();

        let t = rng.random_range(0.0..=0.3);
        let latitude = origin_lat + t * (destination_lat - origin_lat);
        let longitude = origin_lon + t * (destination_lon - origin_lon);
        let (fuel_factor, altitude_range, speed_range, current_mode) =
            Self::determine_by_flight_phase(t, &mut rng);

        Self {
            last_update: Utc::now(),
            fuel_remaining: (max_fuel * fuel_factor / 100).max(100),
            latitude,
            longitude,
            heading: calculate_heading(origin_lat, origin_lon, destination_lat, destination_lon),
            altitude: rng.random_range(altitude_range),
            speed: rng.random_range(speed_range),
            current_mode,
        }
    }

    fn determine_by_flight_phase(
        t: f32,
        rng: &mut rand::prelude::ThreadRng,
    ) -> (u32, std::ops::Range<Meters>, std::ops::Range<KmH>, Mode) {
        match t {
            0.0..=0.2 => (
                90 - (t * 10.0) as u32,      // 90% to 100% fuel for climbing
                SEA_LEVEL..MIN_CRUISING / 2, // Altitude for climbing should not go negative
                200..400,                    // Speed range
                Mode::Climbing,
            ),
            0.8..=0.9 => (
                rng.random_range(10..=50), // 10% to 50% fuel for descending
                SEA_LEVEL..MIN_CRUISING,   // Altitude for descending should not go negative
                200..400,                  // Speed range
                Mode::Descending,
            ),
            _ => (
                rng.random_range(40..=60),  // 40% to 60% fuel for cruising
                MIN_CRUISING..MAX_ALTITUDE, // Higher altitude for cruising
                600..900,                   // Higher speed range
                Mode::Cruising,
            ),
        }
    }

    pub fn simulate(&mut self, destination_lat: Degrees, destination_lon: Degrees) {
        let mut rng = rand::rng();

        // Check for landing completion and update the mode if necessary

        if self.sos(&mut rng) {
            return;
        }

        // Update altitude, speed, and mode based on the distance to the destination
        let distance_to_destination = haversine_distance(
            self.latitude,
            self.longitude,
            destination_lat,
            destination_lon,
        );
        (self.altitude, self.speed, self.current_mode) =
            self.update(distance_to_destination, &mut rng);

        // Move the plane closer to the destination based on current speed
        let t = self.speed as f32 / 32000.0; // Movement factor
        self.latitude += t * (destination_lat - self.latitude);
        self.longitude += t * (destination_lon - self.longitude);
        
        // Update heading to face the destination
        self.heading = calculate_heading(
            self.latitude,
            self.longitude,
            destination_lat,
            destination_lon,
        );

        // Safely decrement fuel
        self.fuel_remaining = self.fuel_remaining.saturating_sub(rng.random_range(1..=5));
    }

    fn update(&self, distance_to_destination: f32, rng: &mut ThreadRng) -> (Meters, KmH, Mode) {
        if distance_to_destination < 1.0 {
            return (SEA_LEVEL, 0, Mode::OnGround);
        }
        if distance_to_destination < 2.0 {
            return (
                self.altitude
                    .saturating_sub(rng.random_range(100..=500))
                    .max(100)
                    .saturating_sub(rng.random_range(10..=30)),
                self.speed
                    .saturating_sub(rng.random_range(200..=1000))
                    .max(300)
                    .saturating_sub(rng.random_range(5..=10)),
                Mode::Landing,
            );
        }
        if distance_to_destination < 20.0 {
            return (
                self.altitude
                    .saturating_sub(rng.random_range(100..=700))
                    .max(300)
                    .saturating_sub(rng.random_range(1..=5)),
                self.speed
                    .saturating_sub(rng.random_range(10..=100))
                    .max(400)
                    .saturating_sub(rng.random_range(50..=100)),
                Mode::Descending,
            );
        }
        if self.altitude < MIN_CRUISING {
            return (
                (self.altitude + rng.random_range(1000..=2000)),
                (self.speed + rng.random_range(10..=80)),
                Mode::Climbing,
            );
        }
        (
            (self.altitude + rng.random_range(0..120) - 50).clamp(MIN_CRUISING, MAX_ALTITUDE),
            (self.speed + rng.random_range(0..=110) - 50),
            Mode::Cruising,
        )
    }

    pub fn landed(&self) -> bool {
        self.current_mode == Mode::OnGround
    }

    pub fn sos(&mut self, rng: &mut ThreadRng) -> bool {
        if self.fuel_remaining > 0 {
            return false;
        }
        if self.speed == 0 && self.altitude == SEA_LEVEL {
            self.current_mode = Mode::OnGround;
            return true;
        }
        self.current_mode = Mode::Sos;
        self.altitude = self.altitude.saturating_sub(rng.random_range(50..=200));
        self.speed = self.speed.saturating_sub(rng.random_range(1..=5));
        true
    }
}

/// Haversine formula to calculate the distance between two points on the globe
pub fn haversine_distance(lat1: Degrees, lon1: Degrees, lat2: Degrees, lon2: Degrees) -> f32 {
    let earth_radius_km = 6371.0;

    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();

    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();

    let a = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    earth_radius_km * c
}

fn calculate_heading(
    origin_lat: Degrees,
    origin_lon: Degrees,
    destination_lat: Degrees,
    destination_lon: Degrees,
) -> Degrees {
    // Convert degrees to radians
    let lat1 = origin_lat.to_radians();
    let lon1 = origin_lon.to_radians();
    let lat2 = destination_lat.to_radians();
    let lon2 = destination_lon.to_radians();

    // Calculate the difference in longitude
    let delta_lon = lon2 - lon1;

    // Calculate the initial bearing
    let y = (delta_lon).sin() * lat2.cos();
    let x = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * delta_lon.cos();
    let mut bearing = y.atan2(x).to_degrees();

    // Normalize the bearing to 0-360 degrees
    bearing = (bearing + 360.0) % 360.0;

    bearing
}
