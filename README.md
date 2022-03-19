AURN API
---

**BETA**

**Joe Hayward (j.d.hayward@surrey.ac.uk)**

**COPYRIGHT 2021, Joe Hayward**

**GNU General Public License v3.0**

This program was designed, tested and run on Ubuntu 21.04. It will very likely run on other Linux distributions though it has not been tested. This program has not been tested on Windows or MacOS.

---

## Table of Contents

1. [Standard Operating Procedure](#standard-operating-procedure)
2. [Settings](#settings)
3. [Setup](#setup)
4. [API](#api)
5. [Available Pollutant Tags](#available-pollutant-tags)

---

## Standard Operating Procedure

### Terminal

This program is initialised via the terminal:
- `bash run.sh` or `./run.sh` 
It then asks for a start year and end year in YYYY format. Some other formats can be used though are not recommended as only the year will be used in date calculation.
Once the program is initialised, the opening blurb will show. If Debug Stats is set to true, it will display all information contained in config.json

---

## Settings

### config.json

config.json contains several configurable parameters for the program:

- TBA

---

## Setup

### Step 1: Download program from Github

Navigate to the directory you want to store the program in and run `git clone https://github.com/Joppleganger/AURN-API.git`

### Step 2: Run setup script

`bash venv_setup.sh` or `./venv_setup.sh` runs the setup script, installing the virtual environment needed to run the program

---

## API

### [main.py](./main.py)
The main script used to run the program, utilises modules found in [modules](./modules) using config specified in [Settings](./Settings)

#### Command line arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
| -s / --start-date | `str` | Date to begin data download (YYYY-MM-DD) | Y | None |
| -e / --end-date | `str` | Date to end data download (YYYY-MM-DD) | Y None |
|-c / --config | `str` | Alternate path to config file, use `/` in pleace of `\` | N | Settings.config.json |

#### Functions

##### parse_date_string

Parses input string and returns `datetime` object. The string can have the following formats (see [strftime](http://strftime.org) for more info):
|Simplified|strftime|
|---|---|
|YYYY|%Y|
|YYYY-MM|%Y-%m|
|YYYY/MM|%Y/%m|
|YYYY\MM|%Y\%m|
|YYYY.MM|%Y.%m|
|YYYY-MM-DD|%Y-%m-%d|
|YYYY/MM/DD|%Y/%m/%d|
|YYYY\MM\DD|%Y\%m\%d|
|YYYY.MM.DD|%Y.%m.%d|

###### Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*date_string*|`str`|The string to be parsed in to a `datetime` object|Y|None|

###### Returns

`datetime object parsed from *date_string*

###### Raises

|Error Type|Cause|
|---|---|
|`ValueError`|*date_string* does not match any of the valid formats|

##### fancy_print

Makes a nicer output to the console

###### Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*str_to_print*|`str`|String that gets printed to console|Y|None|
|*length*|`int`|Character length of output|N|70|
|*form*|`str`|Output type (listed below)|N|NORM|
|*char*|`str`|Character used as border, should only be 1 character|N|\U0001F533 (White box emoji)|
|*end*|`str`|Appended to end of string, generally should be `\n` unless output is to be overwritten, then use `\r`|N|\r|
|*flush*|`bool`|Flush the output stream?|N|False|

**Valid options for _form_**
| Option | Description |
|---|---|
|TITLE|Centres the string, one char at start and end|
|NORM|Left aligned string, one char at start and end|
|LINE|Prints line of *char* of specified *length*|

##### get_json

Open json file and return as dict

###### Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*path_to_json*|`str`|The path to the json file, can be relative e.g Settings/config.json|Y|None|

###### Returns

`dict` containing contents of json file

###### Raises

|Error Type|Cause|
|---|---|
|`FileNotFoundError`|File is not present|
|`ValueError`|Formatting error in json file, such as ' used instead of " or comma after last item|

### [aurn.py](./modules/aurn.py)

Scrapes data from the DEFRA website to download metadata and measurements made by the Automatic Urban and Rural Network

#### Classes

##### AURNAPI

Handles communication with the AURN/DEFRA website to get metadata and measurements

###### Attributes

|Attribute|Type|Description|
|---|---|---|
|*config*|`dict`|Contains user-defined config information|
|*metadata*|`list`|Contains `dict`s containing metadata for AURN sites that were active in the specified date range. Each dict represents a site. The metadata is split in to "tags" for all text info (Site Name etc) and "fields" for location info (Latitude etc)|
|*measurement_csvs*|`defaultdict`|Contains all measurements downloaded from the AURN website, split by year and then station. Should be cleared regularly to avoid memory issues|
|*measurement_jsons*|`defaultdict`|Contains all measurements downloaded from the AURN website formatted for export to an InfluxDB 2.x database, splut by year and then station. Should be cleared regularly to avoid memory issues|

###### Methods

**get_metadata**

Downloads metadata from AURN website. As there's no official Python API for the AURN, this function scrapes the AURN/DEFRA website for a csv containing all metadata for all station in the network and the download link for csv files in the network.

- Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*start_year*|`int`|The first year the measurement download will cover|Y|None|
|*end_year*|`int`|The last year the measurement download will cover|Y|None|

**get_csv_measurements**

Downloads csvs from the AURN website, remove unwanted pollutants and reformat tags in to a nicer format by removing brackets etc.

- Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*download_code*|`str`|The download code for the station|Y|None|
|*year*|`str`|The year you want to download data for, YYYY format|Y|None|

- Returns

None if no csv can be downloaded, no return function otherwise

**csv_to_json_list**

Converts the formatted measurement csv to a list of jsons which an be exported to an InfluxDB 2.x database

- Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*metadata*|`dict`|Metadata for station, contains tags and fields for InfluxDB 2.x|Y|None|
|*download_code*|`str`|The download code for the site, used to find csv in *measurement_csvs* attribute|Y|None|
|*year*|`str`|The year the measurements were made|Y|None|

**csv_as_text**

Returns dataframe as text

- Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*download_code*|`str`|The download code for the site, used to find csv in *measurement_csvs* attribute|Y|None|
|*year*|`str`|The year the measurements were made|Y|None|

**csv_save**

Save csv file to path

- Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*path*|`str`|Path to save the csv to|Y|None|
|*download_code*|`str`|The download code for the site, used to find csv in *measurement_csvs* attribute|Y|None|
|*year*|`str`|The year the measurements were made|Y|None|

**clear_measurement_csvs**

Clear measurement_csvs to reduce memory usage

**clear_measurement_jsons**

Clear measurement_jsons to reduce memory usage

#### Methods

##### remove_brackets

Removed brackets and the contents within them from an input string

###### Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*string_with_brackets*|`str`|Input string to have its brackets removed|Y|None|

###### Returns

Input string with brackets and their contents removed

### [influxwrite.py](./modules/influxwrite.py)

Contains functions and classes pertaining to writing data to InfluxDB 2.x database

#### Classes

##### InfluxWriter

Handles connection and export to InfluxDB 2.x database

###### Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*influx_config*|`dict`|Contains all info relevant to connecting to InfluxDB database|

###### Attributes

| Attribute | Type | Description |
|---|---|---|
|*config*|`dict`|Config info for InfluxDB 2.x database|
|*client*|`InfluxDBClient`|Client object for InfluxDB 2.x database|
|*write_client*|`InfluxDBClient.write_api`|Write client object for InfluxDB 2.x database|

###### Methods

**write_container_list

Writes list of measurement containers to InfluxDB 2.x database, synchronous write used as asynchronous write caused memory issues on a 16 GB machine.

- Keyword Arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*list_of_containers*|`list`|Exports list of data containers to InfluxDB 2.x database|

Containers must have the following keys:
|Key|Description|
|---|---|
|*time*|Measurement time in datetime format|
|*measurement*|Name of measurement in the bucket|
|*fields*|Measurements made at *time*|
|*tags*|Metadata for measurements made at *time*|

- Returns
None

### [timetools.py](./modules/timetools.py)

Temporary class used for time based calculations, will be replaced eventually

#### Classes

##### TimeCalculator

Used for time based calculations

###### Keyword arguments

| Argument | Type | Usage | Required? | Default |
|---|---|---|---|---|
|*date_start*|`datetime`|Start date|Y|None|
|*date_end*|`datetime`|End date|Y|None|

###### Attributes

| Attribute | Type | Description |
|---|---|---|
|*start*|`datetime`|Start date|
|*end*|`datetime`|End date|

###### Methods

**day_difference**

Calculates days between *start* and *end*

- Keyword Arguments
None

- Returns
`int` representing number of days between *start* and *end*

**week_difference**

Calculates weeks between *start* and *end*

- Keyword Arguments

None

- Returns

`int` representing number of days between *start* and *end*

**year_difference**

Calculates years between *start* and *end*

- Keyword Arguments

None

- Returns

`int` representing number of days between *start* and *end*


---

## Available Pollutant Tags

### AURN
- Nitric oxide
- Nitrogen dioxide
- Nitrogen oxides as nitrogen dioxide
- Ozone
- PM2.5 particulate matter
- Volatile PM2.5
- Non-volatile PM2.5
- Daily measured PM2.5
- PM10 particulate matter
- Volatile PM10
- Non-volatile PM10
- Daily measured PM10
- Sulphur dioxide

### Other
Some other pollutants are downloaded from other networks, they can also be saved if required
- n-heptane
- cis-2-butene
- n-pentane
- ethyne
- 1,3,5-trimethylbenzene
- n-butane
- 1,3-butadiene
- 1,2,4-trimethylbenzene
- m+p-xylene
- n-hexane
- isoprene
- 1-butene
- ethene
- ethylbenzene
- 1,2,3-trimethylbenzene
- trans-2-butene
- o-xylene
- propane
- iso-butane
- n-octane
- toluene
- ethane
- 1-pentene
- propene
- trans-2-pentene
- benzene
- iso-pentane
- 2-methylpentane
- iso-octane
