Ok, now I am providing you with the input information to create our new asset. In your response, simply add the new asset to the existing code, we will update that later. You will need to send 1. an updated mta_assets.py file, an updated datasets.py with our new asset added, and updated duckdb_warehouse.py with our new asset added as a new deps. Remember, I want to make a simple version first, that downloads the entire dataset but does not perform any data processing on it. This simple asset should print its columns and a sample of three rows. We will use this output to help us know the correct schema to create our processing function. 

To create our new asset, I will be providing you with the following information

1. name of our new asset
2. endpoint url
3. column we will be using for date for the order clause, which should be ASC
4. The data_url to add as metadata for the asset
