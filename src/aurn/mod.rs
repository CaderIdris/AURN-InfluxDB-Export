//! Module for communicating with UKAIR website, scraping metadata from there and downloading csv
//! measurements
//!
//! This is a rewrite of a Python3 module for the purposes of practising Rust



use std::collections::HashMap;

use chrono::{DateTime, Utc, MIN_DATETIME};
use csv::Reader;
use regex::Regex;
use reqwest::blocking::get;
use scraper::Html;
use scraper::Selector;


type CSVRow = HashMap<String, String>;

/// Queries the UKAIR website for AURN monitoring station metadata
///
/// Downloads a csv containing metadata for all AURN monitoring stations, uses the metadata to
/// generate URLs pointing to csvs containing hourly averages of yearly measurements 
pub struct AURNMetadata {
    metadata: Vec<CSVRow>,
}

impl AURNMetadata {
    /// Initialises an instance of the struct, populating the metadata field with information
    /// parsed from the UKAIR/AURN website. Returns initialised instance of the class.
    ///
    /// The URLs used to query/scrape the data from the UKAIR/AURN website are comprised of
    /// domains, queries and URNs contained in the ukair_config HashMap. "Domain", "Metadata Query"
    /// and "Site Info URN" should all be string slice values that combine to give a valid URL.
    /// "Regex CSV Link", "Regex Site Id Link" and "Regex Site ID Code" represent regular expressions used to
    /// find the download link to the csv in the result of the metadata query, the link to a
    /// monitoring site containing the site ID which is not present in the metadata csv and the
    /// site ID respectively. The site ID is a necessary variable as it forms part of a link to
    /// pre-formatted measurement csvs for each site, split by year.
    ///
    /// Correct values for these at the time of writing are:
    /// <table>
    ///     <thead>
    ///         <tr>
    ///             <th>Key</th>
    ///             <th>Value</th>
    ///         </tr>
    ///     </thead>
    ///     <tbody>
    ///         <tr>
    ///             <td>Domain</td>
    ///             <td>"https://uk-air.defra.gov.uk/"</td>
    ///         </tr>
    ///         <tr>
    ///             <td>Metadata Query</td>
    ///             <td>"networks/find-sites?site_name=&amp;pollutant=9999&amp;group_id=4&amp;closed=true&amp;country_id=9999&amp;region_id=9999&amp;location_type=9999&amp;search=Search+Network&amp;view=advanced&amp;action=results"</td>
    ///         </tr>
    ///         <tr>
    ///             <td>Site Info AURN</td>
    ///             <td>"networks/site-info?uka_id="</td>
    ///         </tr>
    ///         <tr>
    ///             <td>Regex CSV Link</td>
    ///             <td>r#"a[class="bCSV"]"#</td>
    ///         </tr>
    ///         <tr>
    ///             <td>Regex Site Id Link</td>
    ///             <td>r"\?site_id="</td>
    ///         </tr>
    ///         <tr>
    ///             <td>Regex Site ID Code</td>
    ///             <td>r"\w*?$"</td>
    ///         </tr>
    ///     </tbody>
    /// </table>
    ///
    /// # Arguments
    /// * `ukair_config` - (`&HashMap<&str, &str>`) A HashMap containing all string slices necessary to
    /// generate URLs and queries to obtain metadata for all stations in AURN
    ///
    /// # Panics
    /// TBA
    ///
    /// # Examples
    ///
    /// ```
    /// // Initialise an instance of the AURNMetadata struct without metadata. Metadata will be
    /// // downloaded upon initialisation
    /// // Should be mutable if select_between_dates will be used
    /// let mut aurn: AURNMetadata = AURNMetadata::new(&ukair_config);
    ///
    /// ```
    pub fn new(ukair_config: &HashMap<&str, &str>) -> Self {
        let md_query_url: String = ukair_config.get("Domain").unwrap().to_string() + ukair_config.get("Metadata Query").unwrap();
        let mut metadata: Vec<CSVRow> = Vec::new();
        // Download HTML of metadata page and parse it with scraper
        let md_response = get(md_query_url).unwrap().text().unwrap(); 
        let md_page = Html::parse_document(&md_response);
        // Find csv download link and download metadata csv
        let csv_download_link_find = Selector::parse(ukair_config.get("Regex CSV Link").unwrap()).unwrap();
        let csv_download_link = md_page.select(&csv_download_link_find).next().unwrap()
        .value().attr("href").unwrap();
        let csv_string = get(csv_download_link).unwrap().text().unwrap();

        // Iterate over csv, deserialize each row into a Row struct and add to metadata Vec
        let mut csv_reader = Reader::from_reader(csv_string.as_bytes());

        // Regex expressions for finding Site ID within HTML for station
        let site_regex = Regex::new(ukair_config.get("Regex Site ID Link").unwrap()).unwrap();
        let id_regex = Regex::new(ukair_config.get("Regex Site ID Code").unwrap()).unwrap();

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

    /// Removes metadata for any stations that weren't active within specified date range.
    ///
    /// The metadata downloaded contains info from all monitoring stations that full under the AURN
    /// umbrella. Some of these stations have been decommissioned since the 70s so there's little
    /// point in attempting to download 2019 data for them. This function removes any stations that
    /// weren't active within the specified date range to significantly lower the number of HTML
    /// requests that fail due to no data being present.
    ///
    /// # Arguments
    /// * `start_date` - (`DateTime<UTC>`) The date to begin downloading measurements from
    /// * `end_date` - (`DateTime<UTC>`) The date to end measurement downloads at
    ///
    /// # Panics
    /// TBA
    /// Should panic when end_date is before start_date
    ///
    /// # Examples
    ///
    /// ```
    /// // Initialise an instance of the AURNMetadata struct without metadata. Metadata will be
    /// // downloaded upon initialisation
    /// 
    /// let mut aurn: AURNMetadata = AURNMetadata::new(&ukair_config);
    ///
    /// // Remove any stations that fall outside of date range
    ///
    /// aurn.select_between_dates("2017-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(), "2020-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap());
    /// ```
    ///
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
            ("Site Info URN", "networks/site-info?uka_id="),
            ("Regex CSV Link", r#"a[class="bCSV"]"#),
            ("Regex Site ID Link", r"\?site_id="),
            ("Regex Site ID Code", r"\w*?$")

        ]);
       test_config
    }

    /// Checks if metadata downloaded and sites outside of date range are removed
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

