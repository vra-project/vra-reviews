# VRA Auto_Review - Videogame Recommender Algorithm
Program designed to extract and treat user reviews from RAWG.io, with the objective of developing an algorithm able to recommend a videogame to the user based in his likes. It's based on Python 3.9 and works with AWS.

## Table of contents
* [General info] (#general-info)
* [Technologies] (#technologies)
* [Setup] (#setup)

## General info
This project works as an ETL, extracting data from RAWG.io.
In order to do this, the program makes requests to RAWG API, in order to get the newest available review and download them.
When this info is obtained, the data is stored into a S3 bucket.

## Technologies
Project is created with:
* Python 3.9
* BeautifulSoup4 4.11.1
* Pandas 1.4.4
* Requests 2.28.1
* S3fs 2022.10.0

## Setup
To run this project, you'll need to install the libraries noted in requirements.txt.
This project is made to work inside AWS.
A file named secrets.toml containing the S3 Bucket name isn't uploaded.