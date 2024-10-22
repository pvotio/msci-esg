# MSCI ESG Client

## Overview

This project provides a client application to interact with the MSCI API for retrieving and managing ESG data. It handles database connections, data scraping, transformation, and insertion into MSSQL databases, with data exportable as CSV.

## Features

- MSSQL database connection
- Custom scraper engine for data retrieval
- Data transformation and insertion
- Docker setup for easy deployment

## Installation

1. Clone the repo and navigate to the directory.
2. Install dependencies via `pip install -r requirements.txt`.
3. Set up environment variables in `.env` (based on `.env.sample`).
4. Update credentials (App ID, Client ID, Client Secret, MSSQL details).
5. Run the application with `docker build -t msci-esg .` and `docker run msci-esg`, or use `python main.py`.

## Project Structur

- client/ - Scraper engine and API client
- config/ - Logger and app settings
- database/ - Database connections and data insertion
- transformer/ - Data transformation logic
- main.py - Application entry point
