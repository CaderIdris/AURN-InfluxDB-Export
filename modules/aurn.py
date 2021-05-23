import requests as req
from lxml import html  # Needed to scrape AURN website for metadata
import pandas as pd
import datetime as dt 

class AURNAPI:
    """
    """
    def __init__(self, config):
        """
        """
        self.config = config
        self.metadata = list()

    def get_metadata(self, start_year, end_year):
        """
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
        metadata_csv_link_xpath = metadata_html_source.xpath(
                self.config["XPath to CSV"]
                )
        metadata_csv_link = metadata_csv_link_xpath[0]

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
                download_code = None
                if self.config['AURN Data Link'] in site_info_link:
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

        
