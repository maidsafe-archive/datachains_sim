use std::str::FromStr;

#[derive(Clone, Copy, Debug)]
pub enum Strategy {
    Always,
    Complete,
}

impl FromStr for Strategy {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "always" => Ok(Strategy::Always),
            "complete" => Ok(Strategy::Complete),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Params {
    pub init_age: u8,
    pub split_strategy: Strategy,
    pub norejectyoung: bool,
    pub growth: (u8, u8),
    pub structure_output_file: Option<String>,
}
