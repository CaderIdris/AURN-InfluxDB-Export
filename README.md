AURN API
---

**WIP**

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
5. [Components](#components)

---

## Standard Operating Procedure

### Terminal

This program is initialised via the terminal:
- `bash run.sh` or `./run.sh` 
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

TBA

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
