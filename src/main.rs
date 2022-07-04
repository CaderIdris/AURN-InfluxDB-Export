use std::fs;

use serde::Deserialize;

mod aurn;

#[derive(Deserialize, Debug)]
struct Config {
    domain: String,
    metadata_query: String
}

fn main() {
    let config_file = fs::read_to_string("config.toml");
    let config: Config = match config_file {
        Ok(file) => {
            toml::from_str(&file).unwrap()
        },
        Err(_) => {
            Config {
                domain: "https://uk-air.defra.gov.uk".to_string(),
                metadata_query: "/networks/find-sites?site_name=&pollutant=9999&group_id=4\
                        &closed=true&country_id=9999&region_id=9999&location_type=9999\
                        &search=Search+Network&view=advanced&action=results".to_string(),
            }
        },
    };
    dbg!(config);
}
