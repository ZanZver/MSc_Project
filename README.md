 pip install -r requirements.txt

----------------------------------------------------------------------------------------------------------------

Create fake data:
python3 CreateData.py

Small:
1000

Medium:
20000

Large:
100000

----------------------------------------------------------------------------------------------------------------

Install environment:
1) Go into Src/
2) Create conda env (without anything preinstalled):
    conda create --name BobCatLite --no-default-packages python=3.12 
3) Activate environment
    conda activate BobCatLite
4) Install libraries (Found in Src/)
    pip3 install -r requirements.txt 

----------------------------------------------------------------------------------------------------------------

# Testing

## Startup docker
0) Navigate to Docker folder
```/Docker```
1) Go into ethereum
```cd ethereum```
1.1) Remove the data if it existss
```rm -r Data ```
1) Start docker
```docker-compose up -d```
1) Switch to db 
```cd ../db```
3.1) Remove the data if it exists
```rm -r Data```
1) Start the docker
```docker-compose up -d```

## Load the data
Open blockchain.ipynb notebook and execute:
- Create connection
- Read data from parquet
- Save data to blockchain

Open db.ipynb notebook and execute:
- Prepare data
- Create table
- Insert data

## Start the API
0) Navigate to Src/API 
```Src/API/```
1) Start the API
```uvicorn api:app --reload```

## Test the API
Test on: http://127.0.0.1:8000/docs#/

### Test 1
Check if record can be seen.
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/blockchain/latest-record```
&nbsp;&nbsp;&nbsp;&nbsp; Key:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

### Test 2
Update the record:
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/blockchain/append-data```
&nbsp;&nbsp;&nbsp;&nbsp; Request body:
```
{
  "key": "82HFE9767U326DEZ2",
  "key_field": "vin",
  "data": {
        "vin": "82HFE9767U326DEZ2",
        "license_plate": "WU37 WRN",
        "vehicle_make": "Mitsubishi",
        "vehicle_model": "Outlander",
        "vehicle_year": 2000,
        "full_vehicleInfo": {
            "Year": 2000,
            "Make": "Mitsubishi",
            "Model": "Outlander",
            "Category": "SUV"
        },
        "vehicle_category": "SUV",
        "vehicle_make_model": "Mitsubishi Outlander",
        "vehicle_year_make_model": "2000 Mitsubishi Outlander",
        "vehicle_year_make_model_cat": "2000 Mitsubishi Outlander (SUV)"
    }
}
```

See the update:
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/blockchain/latest-record```
&nbsp;&nbsp;&nbsp;&nbsp; Key:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

### Test 3
See the history
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/blockchain/record-history```
&nbsp;&nbsp;&nbsp;&nbsp; Key:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

### Test 4
Remove the record:
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/blockchain/delete-record```
&nbsp;&nbsp;&nbsp;&nbsp; Key:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

See the history - removed record
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/blockchain/record-history```
&nbsp;&nbsp;&nbsp;&nbsp; Key:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

### Test 5
Get all of the records
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/retrieve/```
&nbsp;&nbsp;&nbsp;&nbsp; Query:
&nbsp;&nbsp;&nbsp;&nbsp; ```SELECT * FROM vehicles;```

### Test 6
Get one record
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/retrieve/```
&nbsp;&nbsp;&nbsp;&nbsp; Query:
&nbsp;&nbsp;&nbsp;&nbsp; ```SELECT * FROM vehicles WHERE vin = %s```
&nbsp;&nbsp;&nbsp;&nbsp; Params:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

Update the record
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/update/```
&nbsp;&nbsp;&nbsp;&nbsp; Update_values:
&nbsp;&nbsp;&nbsp;&nbsp; ```{"vehicle_make": "Toyota", "vehicle_model": "Camry"}```
&nbsp;&nbsp;&nbsp;&nbsp; Condition:
&nbsp;&nbsp;&nbsp;&nbsp; ```vin = %s```
&nbsp;&nbsp;&nbsp;&nbsp; Params:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

See the update
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/retrieve/```
&nbsp;&nbsp;&nbsp;&nbsp; Query:
&nbsp;&nbsp;&nbsp;&nbsp; ```SELECT * FROM vehicles WHERE vin = %s```
&nbsp;&nbsp;&nbsp;&nbsp; Params:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

### Test 7
Get the data
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/retrieve/```
&nbsp;&nbsp;&nbsp;&nbsp; Query:
&nbsp;&nbsp;&nbsp;&nbsp; ```SELECT * FROM vehicles WHERE vin = %s```
&nbsp;&nbsp;&nbsp;&nbsp; Params:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

Delete the record
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/delete/```
&nbsp;&nbsp;&nbsp;&nbsp; Condition
&nbsp;&nbsp;&nbsp;&nbsp; ```vin = %s;```
&nbsp;&nbsp;&nbsp;&nbsp; Params
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

Get the data - removed record
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/retrieve/```
&nbsp;&nbsp;&nbsp;&nbsp; Query:
&nbsp;&nbsp;&nbsp;&nbsp; ```SELECT * FROM vehicles WHERE vin = %s```
&nbsp;&nbsp;&nbsp;&nbsp; Params:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

----------------------------------------------------------------------------------------------------------------

