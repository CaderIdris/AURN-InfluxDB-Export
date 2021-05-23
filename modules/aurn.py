""" Contains classes and methods that scrape data from the DEFRA website
to download metadata and measurements for the AURN

The AURN has no official API for Python so metadata and measurements have to
be obtained by scraping HTML data from the DEFRA website. This module 
handles all communications with the DEFRA website, obtaining metadata and 
measurements

    Classes:
        AURNAPI: Handles communication with the AURN/DEFRA website to get
        metadata and measurements
"""

__author__ = "Joe Hayward"
__copyright__ = "2021, Joe Hayward"
__credits__ = ["Joe Hayward"]
__license__ = "GNU General Public License v3.0"
__version__ = "0.1"
__maintainer__ = "Joe Hayward"
__email__ = "j.d.hayward@surrey.ac.uk"
__status__ = "Alpha"

import requests as req
from lxml import html  # Needed to scrape AURN website for metadata
import pandas as pd
import datetime as dt 

class AURNAPI:
    """ Handles communication with the AURN/DEFRA website to get metadata
    and measurements

    Attributes:
        config (dict): Contains config information from config.json

        metadata (list): Contains dictionaries which house all metadata for
        AURN sites that were active in the specified data range, split in to
        "tags" for all text info (Site Name etc) and "fields" for location 
        info (Latitude etc)

    Methods:
        get_metadata: Download a csv file containing info on all AURN sites,
        use the UK-AIR ID to search the AURN website for the "Download Code"
        for the site (A 2-4 character code that is used in the download url
        for the measurement csvs) and put metadata and download code in to a
        dictionary that gets put in to a list

    """
    def __init__(self, config):
        """Initialises class

        Keyword arguments:
            config (dict): Contains info used in class, configured in 
            config.json
        """
        self.config = config
        self.metadata = list()

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

            metadata_html_source (html object): lxml searchable metadata_html_page

            metadata_csv_link (str): Link to metadata csv, obtained by 
            searching html with an XPath string

            site_start_year (int): The year the site started operating

            site_end_year (int): The year the site ended operating. 
            Set to current year + 1 if still in operation (nan in csv)
            
            start_year_in_range (bool): Do start_year and end_year fall within site_start_year?

            end_year_in_range (bool): Do start_year and end_year fall within site_end_year?

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

            site_info_html_source (html object): lxml searchable site_info_html_page

            site_info_link_xpath (list): List of all links in tbale on 
            site_info_html_page

            download_url (str): Download url for measurement csvs

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

            start_year_in_range = (site_start_year <= start_year <= site_end_year)
            end_year_in_range = (site_start_year <= end_year <= site_end_year)
            data_not_available = (not start_year_in_range and not end_year_in_range)
            # Checks if network started after selected period or ended before
            not_aurn_site = ("AURN" not in str(row["Networks"]))
            # Checks if site is actually AURN, some sites double up with different
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
                download_url = None
                if self.config['AURN Data Link'] in site_info_link:
                    download_url = site_info_link
                    continue
            if download_url is not None:
                self.metadata.append(
                        {
                        "tags": {
                            "UK-AIR ID": row["UK-AIR ID"],
                            "EU Site ID": row["EU Site ID"],
                            "EMEP Site ID": row["EMEP Site ID"],
                            "Site Name": row["Site Name"],
                            "Environment Type": row["Environment Type"],
                            "Zone": row["Zone"],
                            "Download URL": download_url
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

        
