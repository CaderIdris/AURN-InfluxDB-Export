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
        let mut metadata: Vec<CSVRow> = Vec::new();
        
        // Get required variables from ukair_config
        let domain: &str = match ukair_config.get("Domain") {
            Some(domain) => domain,
            None => panic!("Error reading Domain from config file")
        };
        let site_info_urn: &str = match ukair_config.get("Site Info URN") {
            Some(urn) => urn,
            None => panic!("Error reading Site Info URN from config file")
        };
        let csv_link: String = match ukair_config.get("Regex CSV Link") {
            Some(regcsv) => regcsv.to_string(),
            None => panic!("Error reading Regex CSV Link from config file")
        };
        let site_regex: regex::Regex = match ukair_config.get("Regex Site ID Link") {
            Some(regex_string) => Regex::new(regex_string).unwrap(),
            None => panic!("Error reading Regex Site ID Link from config file")
        };
        let id_regex: regex::Regex = match ukair_config.get("Regex Site ID Code") {
            Some(regex_string) => Regex::new(regex_string).unwrap(),
            None => panic!("Error reading Regex Site ID Code from config file")
        };
        let md_query: &str = match ukair_config.get("Metadata Query") {
            Some(query) => query,
            None => panic!("Error getting UK-AIR domain from config file")
        };
        let md_query_url: String = domain.to_string() + md_query;
        // Download HTML of metadata page and parse it with scraper
        let md_page = match _read_html(md_query_url) {
            Some(md_page) => md_page,
            None => panic!("Error reading AURN website. Cannot read HTML file.")
        };
        // Find csv download link
        let csv_download_link = match _get_metadata_csv_link(&md_page, csv_link) {
            Some(csv_download_link) => csv_download_link,
            None => panic!("Cannot find csv link in HTML file")

        };
        // Download csv_file and store it as a Reader object for deserialisation
        let csv_string = match _download_csv(csv_download_link.to_string()) {
            Some(csv_string) => csv_string,
            None => panic!("Cannot download csv file")

        };
        let mut csv_reader: Reader<&[u8]> = Reader::from_reader(csv_string.as_bytes());

        // Regex expressions for finding Site ID within HTML for station
        for result in csv_reader.deserialize() {
            let mut record: CSVRow = match result {
                Ok(rec) => rec,
                Err(error) => panic!("Error translating CSV row to CSVRow Hashmap: {:?}", error)

            };
            let uk_air_id: &str = match record.get("UK-AIR ID") {
                Some(id) => id,
                None => panic!("UKAIR ID not found in csv")
            };
            let site_query_urn: String = site_info_urn.to_string() + uk_air_id;
            let site_query: String = domain.to_string() + &site_query_urn;
            let site_page: Html = match _read_html(site_query) {
                Some(page) => page,
                None => panic!("Could not find html page for {:?}", uk_air_id)
            };
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
    /// * `end_date` - (`DateTime<UTC>`) The date to end measurement downloads at
    /// * `start_date` - (`DateTime<UTC>`) The date to begin downloading measurements from
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

/// Downloads HTML source and parses it with scraper
///
/// As there's no official AURN API, we have to read the HTML of the AURN website to get the
/// metadata for all the different sites in the AURN. This private function reads the HTML from
/// the AURN website and locates the download link for a csv file which contains most
/// information relevant to the AURN monitoring sites.
///
/// # Arguments
/// * `query_url` - (`String`) The URL containing the link to the metadata csv
///
/// # Panics
/// None
///
/// # Examples
///
/// ```
/// // Download HTML of metadata page and parse it with scraper
/// let md_page = match _read_metadata_html(md_query_url) {
///     Some(md_page) => md_page,
///     None => panic!("Error reading AURN website. Cannot read HTML file.")
/// };
///
///
/// ```
fn _read_html(query_url: String) -> Option<Html> {
    let response = get(query_url).unwrap().text().unwrap(); 
    let page = Html::parse_document(&response);
    Some(page)
}

/// Gets the link to the metadata csv file by reading a HTML file downloaded from the AURN website
///
/// With no official AURN API, the link to download the metadata csv must be found by reading the
/// HTML file downloaded from the AURN website and parsing it for the download link. This is
/// currently done by looking for the bCSV class but if the HTML source changes then the "Regex CSV
/// Link" variable in the config file may have to be changed
///
/// # Arguments
/// * 'html_file' - (`Html`) HTML file to be parsed by the selector crate
/// * 'regex_csv_link' - (String) Regex string used to look for the bCSV class in the HTML
/// code. 
///
/// # Panics 
/// If the Regex CSV link is improperly formatted in the config file, or not present at
/// all, the function will panic.
/// If the csv link can't be found in the Html file, the function will panic.
fn _get_metadata_csv_link<'a>(html_file: &'a Html, regex_csv_link: String) -> Option<&'a str> {
    let csv_download_link_find = match Selector::parse(&regex_csv_link) {
        Ok(csv_download_link_find) => csv_download_link_find,
        Err(error) => panic!("Couldn't find csv link in HTML file: {:?}", error)
    };
    let csv_download_link = html_file.select(&csv_download_link_find).next().unwrap()
    .value().attr("href");

    csv_download_link
}

/// Downloads csv file
///
/// Downloads csv file from the internet and returns it as a String 
///
/// # Arguments
/// * 'csv_download_link' - ('String') Link to the csv file to be downloaded 
///
/// # Panics
/// If the csv file cannot be downloaded, the function panics
fn _download_csv(csv_download_link: String) -> Option<String> {
    let csv_string: String = match get(csv_download_link) {
        Ok(csv) => csv.text().unwrap(),
        Err(error) => panic!("Could not download csv file: {:?}", error)
    };
    Some(csv_string)
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

    #[test]
    #[should_panic]
    fn wrong_url_panic() {
        let _panic = match _read_html("Bad URL".to_string()) {
            Some(_nothing) => "This should never happen",
            None => panic!("This should happen")
        };
        assert_eq!(1, 1);
    }

    #[test]
    fn metadata_html_download() {
        let test_config = return_test_config();
        let _html = _read_html(test_config.get("Domain").unwrap().to_string() + test_config.get("Metadata Query").unwrap());
        assert_eq!(1, 1);
    }

}

