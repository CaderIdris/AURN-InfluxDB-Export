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
use regex::Regex;
use reqwest::blocking::get;
use scraper::Html;
use scraper::Selector;


type CSVRow = HashMap<String, String>;

pub struct AURNMetadata {
    metadata: Vec<CSVRow>,
}

impl AURNMetadata {

    pub fn new(ukair_config: &HashMap<&str, &str>) -> Self {
        let md_query_url: String = ukair_config.get("Domain").unwrap().to_string() + ukair_config.get("Metadata Query").unwrap();
        let mut metadata: Vec<CSVRow> = Vec::new();
        // Download HTML of metadata page and parse it with scraper
        let md_response = get(md_query_url).unwrap().text().unwrap(); 
        let md_page = Html::parse_document(&md_response);
        // Find csv download link and download metadata csv
        let csv_download_link_find = Selector::parse(r#"a[class="bCSV"]"#).unwrap();
        let csv_download_link = md_page.select(&csv_download_link_find).next().unwrap()
        .value().attr("href").unwrap();
        let csv_string = get(csv_download_link).unwrap().text().unwrap();

        // Iterate over csv, deserialize each row into a Row struct and add to metadata Vec
        let mut csv_reader = Reader::from_reader(csv_string.as_bytes());

        let site_regex = Regex::new(r"\?site_id=").unwrap();
        let id_regex = Regex::new(r"\w*?$").unwrap();

        for result in csv_reader.deserialize() {
            let mut record: CSVRow = result.unwrap();
            let uk_air_id: &str = record.get("UK-AIR ID").unwrap();
            let site_query_urn: String = ukair_config.get("Site Info URN").unwrap().to_string() + uk_air_id;
            let site_query: String = ukair_config.get("Domain").unwrap().to_string() + &site_query_urn;
            let site_response = get(site_query).unwrap().text().unwrap();
            let site_page = Html::parse_document(&site_response);
            let site_code_find = Selector::parse(r#"a[class="bData"]"#).unwrap();
            let bdata_tags = site_page.select(&site_code_find).map(|x| x.value().attr("href").unwrap());
            for bdata_tag in bdata_tags.into_iter() {
                if site_regex.is_match(bdata_tag) {
                    let site_code = id_regex.captures(bdata_tag).unwrap().get(0).unwrap().as_str();
                    record.insert("Site Code".to_string(), site_code.to_string());
                
                }
            }
            metadata.push(record);
        }
        Self {
            metadata: metadata
        }
    }

    pub fn select_between_dates(&mut self, start_date: DateTime<Utc>, end_date: DateTime<Utc>) {
        let iso8601_regex = Regex::new(r"\d...-\d.-\d.T\d.:\d.:\d.Z").unwrap();
        for site_index in (0..self.metadata.len()).rev() {
            let site = &self.metadata[site_index];
            let site_start_date_str: String = site.get("Start Date").unwrap().to_string() + "T00:00:00Z";
            let site_end_date_str: String = site.get("End Date").unwrap().to_string() + "T00:00:00Z";
            let site_start_date: DateTime<Utc> = match iso8601_regex.is_match(&site_start_date_str) {
                // Start date present
                true => site_start_date_str.parse::<DateTime<Utc>>().unwrap(),
                // No start date present
                false => MIN_DATETIME,
            };
            let site_end_date: DateTime<Utc> = match iso8601_regex.is_match(&site_end_date_str) {
                // Start date present
                true => site_end_date_str.parse::<DateTime<Utc>>().unwrap(),
                // No start date present
                false => Utc::now(),
            };
            // Only use metadata if site was measuring during requested range
            let within_start: bool = start_date < site_end_date;
            let within_end: bool = end_date > site_start_date;
            if !(within_start && within_end) {
                self.metadata.remove(site_index);
            }

        }

    }

}


/// Tests
#[cfg(test)]
mod tests {
    use super::*;
    fn return_test_config() -> HashMap<&'static str, &'static str> {
       let test_config: HashMap<&str, &str> = HashMap::from([
            ("Domain", "https://uk-air.defra.gov.uk/"),
            ("Metadata Query", "networks/find-sites?site_name=&pollutant=9999&group_id=4&closed=true&country_id=9999&region_id=9999&location_type=9999&search=Search+Network&view=advanced&action=results"),
            ("Site Info URN", "networks/site-info?uka_id=")

        ]);
       test_config
    }


    /// Checks whether csv can be downloaded
    #[test]
    fn metadata_downloaded() {
        let test_config = return_test_config();
        let aurn: AURNMetadata = AURNMetadata::new(&test_config);
        assert!(aurn.metadata.len() > 0);
    }

    /// Checks if sites outside of date range are removed
    #[test]
    fn sites_removed() {
        let test_config = return_test_config();
        let mut aurn: AURNMetadata = AURNMetadata::new(&test_config);
        let met_length_init = aurn.metadata.len();
        dbg!(met_length_init);
        aurn.select_between_dates("2017-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(), "2020-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap());
        dbg!(aurn.metadata.len());
        assert!(aurn.metadata.len() < met_length_init);
    }

}

