//! Module for communicating with UKAIR website, scraping metadata from there and downloading csv
//! measurements
//!
//! This is a rewrite of a Python3 module for the purposes of practising Rust


/// Queries the UKAIR website for AURN monitoring station metadata
///
/// Downloads a csv containing metadata for all AURN monitoring stations, uses the metadata to
/// generate URLs pointing to csvs containing hourly averages of yearly measurements 

use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Row {
    #[serde(rename = "UK-AIR ID")]
    uk_air_id: String,
    #[serde(rename = "EU Site ID")]
    eu_site_id: String,
    #[serde(rename = "EMEP Site ID")]
    emep_site_id: Option<String>,
    #[serde(rename = "Site Name")]
    site_name: String,
    #[serde(rename = "Environment Type")]
    environment_type: String,
    #[serde(rename = "Zone")]
    zone: String,
    #[serde(rename = "Start Date")]
    start_date: String,
    #[serde(rename = "End Date")]
    end_date: Option<String>,
    #[serde(rename = "Latitude")]
    latitude: f64,
    #[serde(rename = "Longitude")]
    longitude: f64,
    #[serde(rename = "Northing")]
    northing: f64,
    #[serde(rename = "Easting")]
    easting: f64,
    #[serde(rename = "Altitude (m)")]
    altitude: Option<u32>,
    #[serde(rename = "Networks")]
    networks: String,
    #[serde(rename = "AURN Pollutants Measured")]
    pollutants: Option<String>,
    #[serde(rename = "Site Description")]
    site_description: Option<String>,

}

pub struct AURNMetadata {
    metadata: Vec<Row>,
    csv_string: String,
    
}

impl Default for AURNMetadata {
    fn default() -> AURNMetadata {
        let empty_vec: Vec<Row> = Vec::new();
        AURNMetadata {
            metadata: empty_vec,
            csv_string: "".to_string(),
        }
    }

}

impl AURNMetadata {
    pub fn get_metadata(&mut self, query_url: String) {
        let response = reqwest::blocking::get(&query_url).unwrap().text().unwrap(); 
        let page = scraper::Html::parse_document(&response);
        // Download HTML of metadata page and parse it with scraper
        let csv_download_link_find = scraper::Selector::parse(r#"a[class="bCSV"]"#).unwrap();
        let csv_download_link = page.select(&csv_download_link_find).next().unwrap()
        .value().attr("href").unwrap();
        self.csv_string = reqwest::blocking::get(csv_download_link).unwrap().text().unwrap();
        
    }
}

/// Tests
#[cfg(test)]
mod tests {
    use super::*;

    /// Checks whether csv can be downloaded
    #[test]
    fn csv_downloaded() {
        let mut aurn: AURNMetadata = Default::default();
        aurn.get_metadata("https://uk-air.defra.gov.uk/networks/find-sites?site_name=&pollutant=9999&group_id=4&closed=true&country_id=9999&region_id=9999&location_type=9999&search=Search+Network&view=advanced&action=results".to_string());
        assert!(!aurn.csv_string.eq(""));
    }

}

