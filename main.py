"""
"""

__author__ = "Joe Hayward"
__copyright__ = "2021, Joe Hayward"
__credits__ = ["Joe Hayward"]
__license__ = "GNU General Public License v3.0"
__version__ = "0.1"
__maintainer__ = "Joe Hayward"
__email__ = "j.d.hayward@surrey.ac.uk"
__status__ = "Alpha"

import argparse
import json
import datetime as dt

from modules.influxwrite import InfluxWriter
from modules.aurn import AURNAPI

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
        description="Parses measurements from Nova PM csvs "
        "and writes it to an InfluxDB 2.0 database."
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
    config_path = args["config"]

    fancy_print("", form="LINE")
    fancy_print("Nova PM Sensor To InfluxDB v2.0", form="TITLE")
    fancy_print(f"Author:  {__author__}")
    fancy_print(f"Contact: {__email__}")
    fancy_print(f"Version: {__version__}")
    fancy_print(f"Status:  {__status__}")
    fancy_print(f"License: {__license__}")
    fancy_print("", form="LINE")

    config_settings = get_json(config_path)
    fancy_print(f"Exported settings from {config_path}")
    fancy_print("", form="LINE")

    # Debug stats
    if config_settings["Debug Stats"]:
        fancy_print(f"DEBUG STATS", form="TITLE")
        fancy_print(f"")
        fancy_print(f"config.json", form="TITLE")
        for key, item in config_settings.items():
            if len(str(item)) > 40:
                item = f"{item[:40]}..."
            fancy_print(f"{key}: {item}")
        fancy_print(f"")
        fancy_print("", form="LINE")

    # Connect to InfluxDB 2.0 Datakjkjbase
    #influx = InfluxWriter(config_settings)

    # Get metadata from AURN
    aurn = AURNAPI(config_settings)
    aurn.get_metadata(dt.datetime(2016, 1, 1).year, dt.datetime(2021, 1, 1).year)

    # Loop over years
#    for csv in csv_files:
#        if csv in exported_files:
#            continue
#            # if the csv file has already been exported, skip it
#        fancy_print(f"Analysing {csv}", end="\r", flush=True)
#        nova = NovaPM(f"{config_settings['File Path']}/{csv}")
#        if nova.column_number == 8:
#            nova.old_format()
#        elif nova.column_number == 4:
#            nova.new_format()
#        else:
#            continue
#        fancy_print(f"Uploading {csv}: {len(nova.json_list)}", end="\r", flush=True)
#        influx.write_container_list(nova.json_list)
#        fancy_print(f"Exported {csv}")
#        with open(f"{config_settings['File Path']}/ExportedFiles.txt", 
#                "a") as exported_files_txt:
#            exported_files_txt.write(f"{csv}\n")
#    fancy_print("", form="LINE")
#
        
