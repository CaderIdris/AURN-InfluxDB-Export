""" AURN to InfluxDB 2.x Export

This tool takes measurements form the AURN using the AURN module and exports
them to an InfluxDB 2.x database. As the AURN has no official Python API,
this program hacks one together by scraping the HTML code of the website to
obtain metadata and download csv measurements.

"""

__author__ = "Idris Hayward"
__copyright__ = "2021, Idris Hayward"
__credits__ = ["Idris Hayward"]
__license__ = "GNU General Public License v3.0"
__version__ = "1.0 RC"
__maintainer__ = "Idris Hayward"
__email__ = "CaderIdrisGH@outlook.com"
__status__ = "Stable Release"

import argparse
import json
import datetime as dt

from modules.timetools import TimeCalculator
from modules.aurn import AURNAPI
from modules.influxwrite import InfluxWriter


def parse_date_string(dateString):
    """Parses input strings in to date objects

    Keyword arguments:
        date_string (str): String to be parsed in to date object

    Variables:
        parsable_formats (list): List of formats recognised by
        the program. If none are suitable, the program informs
        the user of suitable formats that can be used instead

    Returns:
        Datetime object equivalent of input

    Raises:
        ValueError if input isn't in a suitable format

    """
    parsableFormats = [
            "%Y",
            "%Y-%m",
            "%Y/%m",
            "%Y\\%m",
            "%Y.%m.%d",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%Y\\%m\\%d",
            "%Y.%m.%d"
            ]
    for fmt in parsableFormats:
        try:
            return dt.datetime.strptime(dateString, fmt)
        except ValueError:
            pass
    raise ValueError(
        f'"{dateString}" is not in the correct format. Please'
        f" use one of the following:\n{parsableFormats}"
    )


def fancy_print(
    str_to_print,
    length=70,
    form="NORM",
    char="\U0001F533",
    end="\n",
    flush=False,
):
    """Makes strings output to the console look nicer

    This function is used to make the console output of python
    scripts look nicer. This function is used in a range of
    modules to save time in formatting console output.

        Keyword arguments:
            str_to_print (str): The string to be formatted and printed

            length (int): Total length of formatted string

            form (str): String indicating how 'str_to_print' will be
            formatted. The options are:
                'TITLE': Centers 'str_to_print' and puts one char at
                         each end
                'NORM': Left justifies 'str_to_print' and puts one char
                        at each end (Norm doesn't have to be specified,
                        any option not listed here has the same result)
                'LINE': Prints a line of 'char's 'length' long

            char (str): The character to print.

        Variables:
            length_slope (float): Slope used to adjust length of line.
            Used if an emoji is used for 'char' as they take up twice
            as much space. If one is detected, the length is adjusted.

            length_offset (int): Offset used to adjust length of line.
            Used if an emoji is used for 'char' as they take up twice
            as much space. If one is detected, the length is adjusted.

        Returns:
            Nothing, prints a 'form' formatted 'str_to_print' of
            length 'length'
    """
    length_adjust = 1
    length_offset = 0
    if len(char) > 1:
        char = char[0]
    if len(char.encode("utf-8")) > 1:
        length_adjust = 0.5
        length_offset = 1
    if form == "TITLE":
        print(
            f"{char} {str_to_print.center(length - 4, ' ')} {char}",
            end=end,
            flush=flush,
        )
    elif form == "LINE":
        print(
            char * int(((length) * length_adjust) + length_offset),
            end=end,
            flush=flush,
        )
    else:
        print(
            f"{char} {str_to_print.ljust(length - 4, ' ')} {char}",
            end=end,
            flush=flush,
        )


def get_json(pathToJson):
    """Finds json file and returns it as dict

    Creates blank file with required keys at path if json file is not
    present

        Keyword Arguments:
            pathToJson (str): Path to the json file, including
            filename and .json extension

        Returns:
            Dict representing contents of json file

        Raises:
            FileNotFoundError if file is not present, blank file created

            ValueError if file can not be parsed
    """

    try:
        with open(pathToJson, "r") as jsonFile:
            try:
                return json.load(jsonFile)
            except json.decoder.JSONDecodeError:
                raise ValueError(
                    f"{pathToJson} is not in the proper"
                    f"format. If you're having issues, consider"
                    f"using the template from the Github repo or "
                    f" use the format seen in README.md"
                )
    except FileNotFoundError:
        raise FileNotFoundError(
            f"{pathToJson} could not be found, use the "
            f"template from the Github repo or use the "
            f"format seen in README.md"
        )


if __name__ == "__main__":
    # Parse incoming arguments
    arg_parser = argparse.ArgumentParser(
        description="Downloads measurements from AURN "
        "and exports them to an InfluxDB 2.0 database."
    )
    arg_parser.add_argument(
        "-s",
        "--start-date",
        type=str,
        help="Year to start data export from",
        default="N/A",
    )
    arg_parser.add_argument(
        "-e",
        "--end-date",
        type=str,
        help="Year to end data export",
        default="N/A",
    )
    arg_parser.add_argument(
        "-c",
        "--config",
        type=str,
        help="Alternate location for config json file (Defaults to "
        "./Settings/config.json)",
        default="Settings/config.json",
    )
    args = vars(arg_parser.parse_args())
    start_date_string = args["start_date"]
    end_date_string = args["end_date"]
    config_path = args["config"]

    # Blurb
    fancy_print("", form="LINE")
    fancy_print("AURN Measurements To InfluxDB v2.0", form="TITLE")
    fancy_print(f"Author:  {__author__}")
    fancy_print(f"Contact: {__email__}")
    fancy_print(f"Version: {__version__}")
    fancy_print(f"Status:  {__status__}")
    fancy_print(f"License: {__license__}")
    fancy_print("", form="LINE")

    # Get dates
    if "N/A" in [start_date_string, end_date_string]:
        raise ValueError(
            "One or more required years not provided, "
            "please provide start (-s) and end (-e) year as arguments"
        )
    start_date = parse_date_string(start_date_string)
    end_date = parse_date_string(end_date_string)
    time_config = TimeCalculator(start_date, end_date)
    number_of_years = time_config.year_difference()

    fancy_print(f"Start: {start_date.strftime('%Y-%m-%d')}")
    fancy_print(f"End: {end_date.strftime('%Y-%m-%d')}")
    fancy_print(f"Iterating over {number_of_years} years")
    fancy_print("", form="LINE")

    # Read config file
    config_settings = get_json(config_path)
    fancy_print(f"Imported settings from {config_path}")
    fancy_print("", form="LINE")

    # Debug stats
    if config_settings["Debug Stats"]:
        fancy_print("DEBUG STATS", form="TITLE")
        fancy_print("")
        fancy_print("config.json", form="TITLE")
        for key, item in config_settings.items():
            if len(str(item)) > 40:
                item = f"{str(item)[:40]}..."
            fancy_print(f"{key}: {item}")
        fancy_print("")
        fancy_print("", form="LINE")

    # Connect to InfluxDB 2.0 Database
    influx = InfluxWriter(config_settings)

    # Get metadata from AURN
    fancy_print("Downloading metadata from DEFRA...", end="\r", flush=True)
    aurn = AURNAPI(config_settings)
    aurn.get_metadata(start_date.year, end_date.year)
    fancy_print(f"{len(aurn.metadata)} stations measuring within date range")
    if config_settings["Debug Stats"]:
        for station in aurn.metadata:
            fancy_print(
                    f"{station['tags']['Site Name']}: "
                    f"{station['tags']['Download Code']}"
                    )
    fancy_print("", form="LINE")

    # Loop over station, then years
    for station in aurn.metadata:
        for year_offset in range(0, number_of_years + 1):
            # Download csv measurements
            year = start_date.year + year_offset
            download_code = station['tags']['Download Code']
            fancy_print(f"Downloading data for {station['tags']['Site Name']}"
                        f" ({year})", end="\r", flush=True)
            aurn.get_csv_measurements(download_code, year)
            if aurn.measurement_csvs[year][download_code] is None:
                aurn.clear_measurement_csvs()
                continue  # If the csv couldn't be found, skip
            # Reformat csv to json list
            fancy_print(f"Exporting data for {station['tags']['Site Name']}"
                        f" ({year})", end="\r", flush=True)
            aurn.csv_to_json_list(station, download_code, year)
            influx.write_container_list(
                    aurn.measurement_jsons[year][download_code]
                    )
            aurn.clear_measurement_csvs()
            aurn.clear_measurement_jsons()
        fancy_print(f"{station['tags']['Site Name']} Finished")
    fancy_print("", form="LINE")
