#!/bin/bash

venv=venv/bin/python3

echo "Start Year? (YYYY): "
read startdate
echo "End Year? (YYYY): "
read enddate

$venv main.py -s startdate -e enddate
