""" Contains classes and methods that scrape data from the DEFRA website
to download metadata and measurements for the AURN

The AURN has no official API for Python so metadata and measurements have to
be obtained by scraping HTML data from the DEFRA website. This module
handles all communications with the DEFRA website, obtaining metadata and
measurements

    Classes:
        AURNAPI: Handles communication with the AURN/DEFRA website to get
        metadata and measurements

    Functions:
        remove_brackets: Removes brackets and their contents from a string
"""

__author__ = "Idris Hayward"
__copyright__ = "2021, Idris Hayward"
__credits__ = ["Idris Hayward"]
__license__ = "GNU General Public License v3.0"
__version__ = "1.0"
__maintainer__ = "Idris Hayward"
__email__ = "CaderIdrisGH@outlook.com"
__status__ = "Stable Release"

import requests as req
from lxml import html  # Needed to scrape AURN website for metadata
import pandas as pd
import datetime as dt
import urllib  # Needed for pandas error when csv not present
from collections import defaultdict  # Easier to work with that dict


def remove_brackets(string_with_brackets):
    """ Removes brackets and the content within them from an input string

    Iterates over an input string character by character, adding characters to
    a new string until an open bracket is encountered. Once it is, characters
    aren't added to new string until a matching closing bracket is encountered.

    Keyword arguments:
        string_with_brackets (str): An input string, presumably with brackets

    Variables:
        bracket_pairs (dict): Keys represent open brackets, items are their
        matching closers

        unbracketed (bool): Is character outside a bracket?

    Returns:
        Input string with brackets removes

    """

    bracket_pairs = {
            "(": ")",
            "[": "]",
            "{": "}",
            "<": ">"
            }
    open_brackets = list(bracket_pairs.keys())

    clean_string = ""
    unbracketed = True
    close_bracket = ""
    for character in string_with_brackets:
        if character in open_brackets:
            unbracketed = False
            close_bracket = bracket_pairs[character]
        if unbracketed:
            clean_string = f"{clean_string}{character}"
        if not unbracketed and character == close_bracket:
            unbracketed = True

    # Sometimes removing brackets leaves trailing spaces. This removes them.
    while clean_string[-1] == " ":
        clean_string = clean_string[:-1]

    return clean_string


class AURNAPI:
    """ Handles communication with the AURN/DEFRA website to get metadata
    and measurements

    Attributes:
        config (dict): Contains config information from config.json

        metadata (list): Contains dictionaries which house all metadata for
        AURN sites that were active in the specified data range, split in to
        "tags" for all text info (Site Name etc) and "fields" for location
        info (Latitude etc)

        measurement_csvs (defaultdict): Contains all measurements downloaded,
        split by year and then by station. This should be cleared regularly
        using clear_measurement_csvs to prevent memory issues

        measurement_jsons: Contains all measurement json lists ready to be
        exported to InfluxDB 2.x instance, split by year then by station.
        Should be cleared regularly using clear_measurtement_jsons to 
        prevent memory issues

    Methods:
        get_metadata: Download a csv file containing info on all AURN sites,
        use the UK-AIR ID to search the AURN website for the "Download Code"
        for the site (A 2-4 character code that is used in the download url
        for the measurement csvs) and put metadata and download code in to a
        dictionary that gets put in to a list

        get_csv_measurements: Download measurements from AURN and formats csvs
        in to nicer format for machine reading

        csv_to_json_list: Converts csvs to list of jsons to be exported to
        InfluxDB v2.0 instance

        csv_as_text: Returns csv as text file for writing

        csv_save: Saves csv measurements to path

        clear_measurement_csvs: Clear measurement_csvs

        clear_measurement_jsons: Clear measurement_jsons

    """
    def __init__(self, config):
        """Initialises class

        Keyword arguments:
            config (dict): Contains info used in class, configured in
            config.json
        """
        self.config = config
        self.metadata = list()
        self.measurement_csvs = defaultdict(dict)
        self.measurement_jsons = defaultdict(dict)

    def get_metadata(self, start_year, end_year):
        """ Downloads metadata from AURN/DEFRA website

        As there's no official Python API for the AURN, this function scrapes
        the AURN/DEFRA website's HTML source for a) a csv containing all
        metadata for all stations in the network and b) The download link for
        csv files in the network

        Keyword arguments:
            start_year (int): The year the measurement download will start
            from

            end_year (int): The last year the measurement download will cover

        Variables:
            metadata_search_url (str): The url used to search for info on all
            stations in the network

            metadata_html_page (request): Returned information from html
            request for metadata

            metadata_html_source (html object): lxml searchable
            metadata_html_page

            metadata_csv_link (str): Link to metadata csv, obtained by
            searching html with an XPath string

            site_start_year (int): The year the site started operating

            site_end_year (int): The year the site ended operating.
            Set to current year + 1 if still in operation (nan in csv)

            start_year_in_range (bool): Do start_year and end_year fall within
            site_start_year?

            end_year_in_range (bool): Do start_year and end_year fall within
            site_end_year?

            data_not_available (bool): Does the date range cross the dates
            the site was active?

            not_aurn_site (bool): Is the site an AURN site? Some sites in the
            csv are duplicates with different names but the same download url,
            one of the duplicates doesn't state it's AURN so this test removes
            them

            uk_air_id (str): The ID code for the station, used to search for
            the download code

            site_info_url (str): The url for the information for a site

            site_info_html_page (request): Returned information for site info
            request

            site_info_html_source (html object): lxml searchable
            site_info_html_page

            site_info_link_xpath (list): List of all links in tbale on
            site_info_html_page

            download_code (str): Download url for measurement csvs

        """

        # Get HTML file with search results of all sites, open or closed,
        # in network
        metadata_search_url = (f"{self.config['AURN Domain']}"
                               f"{self.config['AURN Metadata Search']}")
        metadata_html_page = req.get(
                metadata_search_url,
                headers={"User-Agent": self.config['User Agent']}
                )
        metadata_html_source = html.fromstring(metadata_html_page.content)

        # Search HTML file for link to csv metadata using xpath
        metadata_csv_link = metadata_html_source.xpath(
                self.config["XPath to CSV"]
                )[0]

        # Download metadata csv
        metadata_csv = pd.read_table(metadata_csv_link, sep=",")

        # Get download code (Usually 3 characters) for all sites
        # e.g Aberdeen(UKA00399) is ABD
        for index, row in metadata_csv.iterrows():
            # Check start and end year, move to next if site
            # wasn't active between them
            # Also skip if not explicitly stated as AURN
            if row["Start Date"] != "Unavailable":
                site_start_year = dt.datetime.strptime(
                        row["Start Date"],
                        "%Y-%m-%d"
                        ).year
            else:
                site_start_year = 1990
                # If the start date is unavailable, assume it's early
                # Unsure why unavailable appears for only one site at
                # the time of commenting but this gets around it and
                # any future issues
            if str(row["End Date"]) != "nan":
                site_end_year = dt.datetime.strptime(
                        row["End Date"],
                        "%Y-%m-%d"
                        ).year
            else:
                site_end_year = dt.datetime.now().year + 1

            start_year_in_range = (site_start_year <= start_year <=
                                   site_end_year)
            end_year_in_range = (site_start_year <= end_year <= site_end_year)
            data_not_available = (not start_year_in_range and not
                                  end_year_in_range)
            # Checks if network started after selected period or ended before
            not_aurn_site = ("AURN" not in str(row["Networks"]))
            # Checks if site is actually AURN, some sites double up with
            # different
            # names but one of the duplicates doesn't state AURN
            if data_not_available or not_aurn_site:
                continue

            # Search HTML of site info
            uk_air_id = row['UK-AIR ID']
            site_info_url = (
                    f"{self.config['AURN Domain']}"
                    f"{self.config['AURN Site Info']}"
                    f"{uk_air_id}"
                    f"{self.config['AURN Site Info Provider']}"
                    )
            site_info_html_page = req.get(
                    site_info_url,
                    headers={"User-Agent": self.config['User Agent']}
                    )
            site_info_html_source = html.fromstring(
                    site_info_html_page.content
                    )

            # Get download code from HTML
            site_info_link_xpath = site_info_html_source.xpath(
                    self.config["XPath to Code"]
                    )
            for site_info_link in site_info_link_xpath:
                download_code = None
                if self.config['AURN Site Code Link'] in site_info_link:
                    download_code = site_info_link.split('=')[1]
                    continue
            if download_code is not None:
                self.metadata.append(
                        {
                            "tags": {
                                "UK-AIR ID": row["UK-AIR ID"],
                                "EU Site ID": row["EU Site ID"],
                                "EMEP Site ID": row["EMEP Site ID"],
                                "Site Name": row["Site Name"],
                                "Environment Type": row["Environment Type"],
                                "Zone": row["Zone"],
                                "Download Code": download_code
                            },
                            "fields": {
                                "Latitude": float(row["Latitude"]),
                                "Longitude": float(row["Longitude"]),
                                "Northing": float(row["Northing"]),
                                "Easting": float(row["Easting"]),
                                "Altitude": float(row["Altitude (m)"])
                            }
                        }
                    )

    def get_csv_measurements(self, download_code, year):
        """ Download csvs from AURN website, remove unwanted pollutants and
        reformat them in to a prettier format

        All measurements made by the AURN are stored on their website in
        a preformatted csv. The first stage of this method downloads the csv
        from the data link, appending {Download Code}_{Year}.csv to the end
        to get the measurements for that site for that year in csv format.
        If no data can be found, None is recorded instead of a dataframe
        and the method quits early.

        The second stage of the method involves making the data look nicer.
        Some of the measurement columns include brackets in their names
        (e.g. PM<sub>10</sub>) which are used to render subscript for 
        pollutants when viewed as HTML, some include info on the instrument
        used (e.g (FIDAS)) but not all sites use the same identifiers.
        To maintain standardisation, this info is removed.

        The third stage then removes all unwanted pollutants. If no pollutants
        are listed in the config, all are kept. Otherwise, all pollutants not
        listed are removed, as well as their associated unit and status
        columns.

        The fourth stage converts the time and date columns in to a single
        datetime object. However, the timestamps used by the AURN do not
        conform to ISO8601 or any RFC standard that I could find, they
        appear to be legacy so as to maintain consistency between all csvs.
        Their format is YYYY-MM-DD in 'Date' and HH:MM in 'time'. :00 has
        to be appended to the time column to give it in seconds, otherwise
        pandas will not recognise it. The time has to be converted in to a
        timedelta object and added to the date column (which assumes the time
        is 00:00:00 in absence of a time string to get the correct time as the
        date parser does not understand 24 as a valid hour, which the AURN
        uses instead of 00 the next day for midnight.

        The final stage makes a nicer formatted csv. The AURN csvs do not 
        distinguish between status and unit columns for different pollutants
        (e.g unit columns for NO2 and PM2.5 both have the heading "unit"),
        so the pollutant is prepended on them to make it easier to distinguish
        them. Otherwise, the only distinctions can be made by their position.
        The date and time columns are then replaced with a single datetime 
        columns containing a datetime object. This csv is then added to
        the measurement_csvs dict, nested by year and then by download code.
        
        Keyword arguments:
            download_code (str): The download code found during the metadata
            search

            year (str): The year you want to download data for, YYYY format

        Variables:
            csv_url (str): The url to the formatted csv provided by the AURN

            raw_csv (DataFrame): Formatted csv obtained from AURN website

            raw_columns_list (list): All column names in AURN csv file, this
            list is regenerated a few times and may differ

            debracketed_columns (dict): In order to bulk rename the columns, a
            dictionary can be passed in to the pd.rename function to change
            them all at once. The key is the original name and the value is the
            new one, in this case the original name with brackets and their
            contents removed

            pollutants_to_remove (list): A list containing all pollutants not
            listed in the config as pollutants required by the program

            allowed_pollutants (list): Pollutants specified in the config to
            keep. If empty, all pollutants are kept

            pollutant_not_allowed (bool): Determines if a specified pollutant
            should be kept (False) or removed (True). Auto falses if column is
            unit, status, Date, Time or if the allowed_pollutants list is
            empty

            indices (list): List of indices of the columns to be removed.
            Sorted in descending order so the indices of later columns in
            the list don't change when earlier ones are removed

            columns_to_drop (list): Names of the columns to be removed

            column_names (list): Names of the columns of pollutants to be
            removed, as well as their associated status and unit columns which
            are 1 and 2 indices higher respectively

            dt_col (pd.Series): The new Datetime column, a combination of the
            'Date' and 'time' columns in datetime format

            measurement_csv_data (dict): Dict of lists that represent the data
            to be put in the new csv, keys are the column headers

            measurement_csv (DataFrame): The new, formatted csv

        Returns:
            Returns none if no csv can be found but this is only to exit the
            method quickly without nesting a bunch of if statements and
            making the code look messy
        """
        # Generate url to measurement csv and download
        csv_url = (
                f"{self.config['AURN Domain']}/"
                f"{self.config['AURN Data Link']}/"
                f"{download_code}_{year}.csv"
                )
        try:
            raw_csv = pd.read_table(csv_url, sep=",", skiprows=4,
                                    low_memory=False)
        except urllib.error.HTTPError:
            # If data can't be found, quit out and move on
            self.measurement_csvs[year][download_code] = None
            return None

        # Remove brackets from columns
        # Some csv files have brackets in their pollutant names (e.g
        # PM<sub>10</sub>) while some don't. Removing the brackets
        # standardises this
        raw_columns_list = list(raw_csv.columns)
        debracketed_columns = {}
        for column in raw_columns_list:
            if any(bracket in column for bracket in ["(", "{", "[", "<"]):
                debracketed_columns[column] = remove_brackets(column)
        if len(debracketed_columns.keys()) > 0:
            raw_csv = raw_csv.rename(columns=debracketed_columns)

        # Get valid columns
        raw_columns_list = list(raw_csv.columns)
        pollutants_to_remove = list()
        allowed_pollutants = self.config["Pollutants"]
        for column in raw_columns_list:
            pollutant_not_allowed = (
                column not in allowed_pollutants
                and len(allowed_pollutants) != 0
                and not any(tag in column for tag in [
                        "unit", "status", "Date", "time"
                        ])
                    )
            if pollutant_not_allowed:
                pollutants_to_remove.append(column)

        # Get indices of invalid columns and remove
        indices = list()
        for pollutant in pollutants_to_remove:
            indices.append(raw_columns_list.index(pollutant))
        indices.sort(reverse=True)
        columns_to_drop = list()
        for index in indices:
            column_names = [raw_columns_list[index + offset] for offset in
                            range(0, 3)]
            columns_to_drop.extend(column_names)
        if len(columns_to_drop) > 0:
            raw_csv = raw_csv.drop(columns_to_drop, axis=1)

        # Turn two date and time columns to datetime
        dt_col = pd.to_datetime(raw_csv.pop('Date'), format='%d-%m-%Y') + \
            pd.to_timedelta(raw_csv.pop('time') + ':00')
        dt_col = dt_col.rename('Datetime')
        raw_csv = pd.concat([dt_col, raw_csv], axis=1)
        raw_columns_list = list(raw_csv.columns)

        # Make a new csv that looks nicer
        if (len(raw_columns_list) - 1) % 3 == 0:
            measurement_csv_data = dict()
            measurement_csv_data['Datetime'] = list(raw_csv['Datetime'])
            non_datetime_columns = int((len(raw_columns_list) - 1) / 3)
            for column_index_raw in range(0, non_datetime_columns):
                column_index = (column_index_raw * 3) + 1
                measurement_csv_data[f'{raw_columns_list[column_index]}'
                                     ] = list(raw_csv[
                                         raw_columns_list[column_index]])
                measurement_csv_data[f'{raw_columns_list[column_index]}'
                                     f' status'] = list(raw_csv[
                                         raw_columns_list[column_index+1]])
                measurement_csv_data[f'{raw_columns_list[column_index]}'
                                     f' unit'] = list(raw_csv[
                                         raw_columns_list[column_index+2]])
                measurement_csv = pd.DataFrame(data=measurement_csv_data)
            self.measurement_csvs[year][download_code] = (
                        measurement_csv
                    )

    def csv_to_json_list(self, metadata, download_code, year):
        """ Converts the formatted csv in to a list of jsons which can be
        exported to an InfluxDB 2.x database.
        
        Takes the csv formatted in get_csv_measurements and the metadata
        downloaded in get_metadata and stores it in a list of jsons that
        can be exported to an InfluxDB 2.0 database

        Keyword Arguments:
            metadata (dict): Metadata for station, contains tags and fields
            for InfluxDB

            download_code (str): The download code for the site, used to find
            csv in measurement_csvs

            year (str): The year the measurements were made

        Variables:
            csv_file (DataFrame): The csv to be converted to json list

            column_name_list (list): List of column names in csv

            status_columns (list): Names of all status and unit columns

            measurement_columns (list): Names of all measurement columns

            measurement_container (dict): Dict in the format that InfluxDB
            recognises for data export
        """
        csv_file = self.measurement_csvs[year][download_code]
        column_name_list = list(csv_file.columns)
        status_columns = list()
        measurement_columns = list()
        container_list = list()
        for column in column_name_list:
            if any(tag in column for tag in ["status", "unit"]):
                status_columns.append(column)
            elif "Datetime" not in column:
                measurement_columns.append(column)
        for index, row in csv_file.iterrows():
            measurement_container = {'tags': {}, 'fields': {}}
            measurement_container["time"] = row["Datetime"].to_pydatetime()
            measurement_container["measurement"] = (
                    "Automatic Urban Rural Network"
                    )
            for m_column in measurement_columns:
                try:
                    measure = float(row[m_column])
                    if measure != measure:
                        continue
                    measurement_container["fields"][m_column] = measure
                except TypeError:
                    continue
            for s_column in status_columns:
                status = row[s_column]
                if status == "":
                    continue
                measurement_container["tags"][s_column] = row[s_column]
            for key, value in metadata['tags'].items():
                measurement_container['tags'][key] = value
            for key, value in metadata['fields'].items():
                measurement_container['fields'][key] = value
            container_list.append(
                    measurement_container.copy()
                    )

        self.measurement_jsons[year][download_code] = container_list

    def csv_as_text(self, download_code, year):
        """ Return dataframe as text

        Keyword Arguments:
            download_code (str): Used to locate DataFrame

            year (str): Used to locate DataFrame


        Returns:
            String representation of csv, or blank string if no csv
            present
        """
        if self.measurement_csvs[year][download_code] is not None:
            return self.measurement_csvs[year][download_code].to_csv()
        else:
            return ""

    def csv_save(self, path, download_code, year):
        """ Save csv file to path

        Keyword Arguments:
            path (str): Path to save csv to

            download_code (str): Used to locate DataFrame

            year (str): Used to locate DataFrame
        """
        if self.measurement_csvs[year][download_code] is not None:
            self.measurement_csvs[year][download_code].to_csv(
                    path_or_buf=path
                    )

    def clear_measurement_csvs(self):
        """ Clear measurement_csvs to reduce memory usage
        """
        self.measurement_csvs = defaultdict(dict)

    def clear_measurement_jsons(self):
        """ Clear measurement_jsons to reduce memory usage
        """
        self.measurement_jsons = defaultdict(dict)
