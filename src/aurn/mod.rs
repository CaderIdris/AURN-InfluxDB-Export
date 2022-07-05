//! Module for communicating with UKAIR website, scraping metadata from there and downloading csv
//! measurements
//!
//! This is a rewrite of a Python3 module for the purposes of practising Rust


/// Queries the UKAIR website for AURN monitoring station metadata
///
/// Downloads a csv containing metadata for all AURN monitoring stations, uses the metadata to
/// generate URLs pointing to csvs containing hourly averages of yearly measurements 

use std::collections::HashMap;

use chrono::{DateTime, Utc, MIN_DATETIME};
use csv::Reader;
use reqwest::blocking::get;
use scraper::Html;
use scraper::Selector;


type CSVRow = HashMap<String, String>;

pub struct AURNMetadata {
    metadata: Vec<CSVRow>,
}

impl AURNMetadata {

    pub fn new() -> Self {
        let empty_vec: Vec<CSVRow> = Vec::new();
        Self {
            metadata: empty_vec
        }
    }

    pub fn get_metadata(&mut self, query_url: &str, start_date: DateTime<Utc>, end_date: DateTime<Utc>) {
        // Download HTML of metadata page and parse it with scraper
        let response = get(query_url).unwrap().text().unwrap(); 
        let page = Html::parse_document(&response);
        // Find csv download link and download metadata csv
        let csv_download_link_find = Selector::parse(r#"a[class="bCSV"]"#).unwrap();
        let csv_download_link = page.select(&csv_download_link_find).next().unwrap()
        .value().attr("href").unwrap();
        let csv_string = get(csv_download_link).unwrap().text().unwrap();

        // Iterate over csv, deserialize each row into a Row struct and add to metadata Vec
        let mut csv_reader = Reader::from_reader(csv_string.as_bytes());
        for result in csv_reader.deserialize() {
            let record: CSVRow = result.unwrap();
            let mut site_start_date_str: String = record.get("Start Date").unwrap().to_string() + "T00:00:00Z";
            let mut site_end_date_str: String = record.get("End Date").unwrap().to_string() + "T00:00:00Z";
            println!("{}", site_start_date_str);
            let site_start_date: DateTime<Utc> = match site_start_date_str.len() {
                // Start date present
                20 => site_start_date_str.parse::<DateTime<Utc>>().unwrap(),
                // No start date present
                _ => MIN_DATETIME,
            };
            let site_end_date: DateTime<Utc> = match site_end_date_str.len() {
                // End date present
                20 => site_end_date_str.parse::<DateTime<Utc>>().unwrap(),
                // No end date present
                _ => Utc::now(),
            };
            // Only use metadata if site was measuring during requested range
            let within_start: bool = start_date < site_end_date;
            let within_end: bool = end_date > site_start_date;
            if within_start && within_end {
                self.metadata.push(record);
            }
        }
    }

    pub fn sites_with_measurement_check(&mut self) {


    }
}

/// Tests
#[cfg(test)]
mod tests {
    use super::*;

    /// Checks whether csv can be downloaded
    #[test]
    fn csv_downloaded() {
        let mut aurn: AURNMetadata = AURNMetadata::new();
        aurn.get_metadata("https://uk-air.defra.gov.uk/networks/find-sites?site_name=&pollutant=9999&group_id=4&closed=true&country_id=9999&region_id=9999&location_type=9999&search=Search+Network&view=advanced&action=results", "2017-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(), "2020-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap());
        assert!(aurn.metadata.len() > 0);
    }

}
