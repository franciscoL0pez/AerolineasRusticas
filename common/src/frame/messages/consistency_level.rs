#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsistencyLevel {
    #[default]
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Serial = 0x0008,
    LocalSerial = 0x0009,
    LocalOne = 0x000A,
}

impl ConsistencyLevel {
    pub fn value(self) -> u16 {
        self as u16
    }

    pub fn from_value(val: u16) -> Self {
        match val {
            0x0000 => ConsistencyLevel::Any,
            0x0001 => ConsistencyLevel::One,
            0x0002 => ConsistencyLevel::Two,
            0x0003 => ConsistencyLevel::Three,
            0x0004 => ConsistencyLevel::Quorum,
            0x0005 => ConsistencyLevel::All,
            0x0006 => ConsistencyLevel::LocalQuorum,
            0x0007 => ConsistencyLevel::EachQuorum,
            0x0008 => ConsistencyLevel::Serial,
            0x0009 => ConsistencyLevel::LocalSerial,
            0x000A => ConsistencyLevel::LocalOne,
            _ => Default::default(), // Returns the default value if the input is not recognized
        }
    }

    pub fn from_str_to_enum(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "any" => ConsistencyLevel::Any,
            "one" => ConsistencyLevel::One,
            "two" => ConsistencyLevel::Two,
            "three" => ConsistencyLevel::Three,
            "quorum" => ConsistencyLevel::Quorum,
            "all" => ConsistencyLevel::All,
            "localquorum" => ConsistencyLevel::LocalQuorum,
            "eachquorum" => ConsistencyLevel::EachQuorum,
            "serial" => ConsistencyLevel::Serial,
            "localserial" => ConsistencyLevel::LocalSerial,
            "localone" => ConsistencyLevel::LocalOne,
            _ => Default::default(), // Returns the default value if the input is not recognized
        }
    }
}

impl std::fmt::Display for ConsistencyLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ConsistencyLevel::Any => "any",
            ConsistencyLevel::One => "one",
            ConsistencyLevel::Two => "two",
            ConsistencyLevel::Three => "three",
            ConsistencyLevel::Quorum => "quorum",
            ConsistencyLevel::All => "all",
            ConsistencyLevel::LocalQuorum => "localquorum",
            ConsistencyLevel::EachQuorum => "eachquorum",
            ConsistencyLevel::Serial => "serial",
            ConsistencyLevel::LocalSerial => "localserial",
            ConsistencyLevel::LocalOne => "localone",
        };
        write!(f, "{}", s)
    }
}
