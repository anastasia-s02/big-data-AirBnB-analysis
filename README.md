# AirBnB Project

This repository contains the code and files that created by Anastasia Samoilova for final project for Processing Big Data for Analytic Applications class. 


## Folder overview.
1. `data_ingest` contains file for data ingestion. 
2. `etl_code` contains files for cleaning AirBnB dataset and for joining it with crime dataset. 
3. `profiling_code` contains files that can be run for profiling both joined and cleaned datasets. 
4. `ana_code` contains code for analyzing data and generating insights. Additionally, it has some csv files for analysis and graphs that were generated as a result fo running the code. 
5. `screenshots` contains screenshots of executing files. 


## NYC AirBnB listings instructions:
1. Go to the following link to download the data "Detailed Listings Data" for New York, NY from archive from December 4th 2022: http://insideairbnb.com/get-the-data/
2. Go to as15026 hdfs, and the initial data is already located under `project/listings.csv`
3. Upload the following files into DataProc: 
    1. `nyc_listings_ingest.scala`
    2. `clean_data.scala`
    3. `join_data.scala`
4. Run those files using the following commands to clean data and get the joined dataset that has 2 additional rows from crime data -- non-felony crimes and felony crimes: 
    1. `spark-shell --deploy-mode client -i nyc_listings_ingest.scala`
    2. `spark-shell --deploy-mode client -i clean_data.scala`
    The output from this run is located in `project/output/clean_nyc_listings.csv`. Note that is has been manually renamed for the ease of use. 
    3. `spark-shell --deploy-mode client -i join_data.scala`
    The output from this run is located in `project/join_output/join_data.csv`. Note that is has been manually renamed for the ease of use.
5. Next, download the following files: 
    1. `joined_profiling.scala`
    2. `listings_profiling.scala`
6. Run the following commands to get the profiling information about both the joined dataset and the dataset with listings only:
    1. `spark-shell --deploy-mode client -i joined_profiling.scala`
    2. `spark-shell --deploy-mode client -i listings_profiling.scala`
    The output of these files comes as print out. 
7. Next, download the following file --  `joined_data_analytics.scala` -- it runs linear regression, computes correlation coefficients, and creates csv files with summary data. 
8. Run this file using this command: 
    1. `spark-shell --deploy-mode client -i joined_data_analytics.scala`
    The output can be found in `project/analytics` folder on as15026 hdfs, which contains 12 csv files with summary data (those files have been manually renamed for the ease of use as well). The results of linear regression and correlation coefficients are printed out after the run of the file. 
9. Next, move those files onto your local machine and make sure they are all in the same folder: 
    1. `graphs.py`
    2. `correlations.csv`
    3. `priceCrimes.csv`
    4. `ratingCrimes.csv`
    5. `ratingSuperhost.csv`
    6. `ratingReviews.csv`
10. Enter the following command: `python graphs.py`
    1. After the run of this file, 6 `png` files with graphs, tables, and heatmaps will appear in the same folder. They will be named: 
    `price_vs_crimes.png`, `rating_vs_crimes.png`, `rating_vs_superhost.png`, `rating_vs_reviews.png`, `rating_vs_price.png`, `heatmap_correlations.png`. 
