#[repr(u8)] // byte
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Version {
    RequestV3 = 0x03,
    ResponseV3 = 0x83,
}

impl TryFrom<u8> for Version {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x03 => Ok(Version::RequestV3),
            0x83 => Ok(Version::ResponseV3),
            _ => Err("Unsupported version"),
        }
    }
}

impl From<Version> for u8 {
    fn from(version: Version) -> Self {
        version as u8
    }
}
