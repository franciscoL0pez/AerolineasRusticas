#[derive(Debug, PartialEq, Clone)]
pub enum Mode {
    OnGround,
    Cruising,
    Climbing,
    Descending,
    Landing,
    Sos,
    Unknown,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::OnGround => write!(f, "on_ground"),
            Mode::Cruising => write!(f, "cruising"),
            Mode::Climbing => write!(f, "climbing"),
            Mode::Descending => write!(f, "descending"),
            Mode::Landing => write!(f, "landing"),
            Mode::Sos => write!(f, "sos"),
            Mode::Unknown => write!(f, "unknown"),
        }
    }
}
