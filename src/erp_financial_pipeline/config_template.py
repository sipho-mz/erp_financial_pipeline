"""
config.py — Central configuration for the ERP Financial Data Platform.

All other modules import from here. Change a value once, it updates everywhere.
"""
from pathlib import Path

PROJECT_ROOT = "" #Path to root folder

DATA_DIR = "" #Path to data folder
BRONZE_DIR = "" #Path to bronze folder
SILVER_DIR = "" #Path to silver folder
GOLD_DIR = "" #Path to gold folder

BASE_API_URL = "" 

ENDPOINTS = {} #Define the endpoints

FISCAL_YEARS ={} #Dict of years

PAGE_SIZE = 0 #Define page size

