# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f7983fab-86ae-4f49-97b6-829aba461a88",
# META       "default_lakehouse_name": "lh_earthquake_all",
# META       "default_lakehouse_workspace_id": "4bab1027-76e9-421b-9021-ad37d26a5e61",
# META       "known_lakehouses": [
# META         {
# META           "id": "f7983fab-86ae-4f49-97b6-829aba461a88"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Importing Earthquake Data

# MARKDOWN ********************


# CELL ********************

from datetime import date, timedelta 

start_date = date.today() - timedelta(7)
end_date = date.today() - timedelta(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

# Construct the API URL with start and end dates provided by Data Factory, formatted for geojson output.
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

# Make the GET request to fetch data
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Get the JSON response
    data = response.json()
    data = data['features']
    
    # Specify the file name (and path if needed)
    file_path = f'/lakehouse/default/Files/{start_date}_earthquake_data.json'
    
    # Open the file in write mode ('w') and save the JSON data
    with open(file_path, 'w') as file:
        # The `json.dump` method serializes `data` as a JSON formatted stream to `file`
        # `indent=4` makes the file human-readable by adding whitespace
        json.dump(data, file, indent=4)
        
    print(f"Data successfully saved to {file_path}")
else:
    print("Failed to fetch data. Status code:", response.status_code)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
