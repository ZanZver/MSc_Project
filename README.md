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
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/retrieve/all/```

### Test 6
Get one record
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/retrieve/specific/```
&nbsp;&nbsp;&nbsp;&nbsp; Key:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

Update the record
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/update/```
&nbsp;&nbsp;&nbsp;&nbsp; update_values:
&nbsp;&nbsp;&nbsp;&nbsp; ```{"vehicle_make": "Toyota", "vehicle_model": "Camry"}```
&nbsp;&nbsp;&nbsp;&nbsp; key:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

See the update
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/retrieve/specific/```
&nbsp;&nbsp;&nbsp;&nbsp; key:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

### Test 7
Get the data
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/retrieve/specific/```
&nbsp;&nbsp;&nbsp;&nbsp; key:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

Delete the record
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/delete/```
&nbsp;&nbsp;&nbsp;&nbsp; key
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

Get the data - removed record
&nbsp;&nbsp;&nbsp;&nbsp; Path:
&nbsp;&nbsp;&nbsp;&nbsp; ```/db/retrieve/```
&nbsp;&nbsp;&nbsp;&nbsp; key:
&nbsp;&nbsp;&nbsp;&nbsp; ```82HFE9767U326DEZ2```

----------------------------------------------------------------------------------------------------------------

# Testing
## Performance testing

Navigate to Testing/API and start the program with:
```locust``

Then open 127.0.0.0:8089 to run the tests

## Unit testing

Start unit testing with (make sure to be in root dir):
```pytest -s ./Testing/Unit/test_bc.py```

----------------------------------------------------------------------------------------------------------------
<!-- docker pull hyperledger/fabric-ca:latest
docker pull hyperledger/fabric-peer:latest
docker pull hyperledger/fabric-orderer:latest


docker-compose up -d -->


curl -sSL https://bit.ly/2ysbOFE | bash -s

docker-compose up


brew install go

git clone https://github.com/hyperledger/fabric-samples.git
cd fabric-samples/test-network
curl -sSL https://bit.ly/2ysbOFE | bash -s
./network.sh up createChannel -c mychannel -ca


# T9
Install:
https://hyperledger-fabric.readthedocs.io/en/release-2.5/prereqs.html

curl -sSLO https://raw.githubusercontent.com/hyperledger/fabric/main/scripts/install-fabric.sh && chmod +x install-fabric.sh

./install-fabric.sh docker samples binary

cd fabric-samples/test-network
#./network.sh up
./network.sh createChannel -c mychannel
./network.sh deployCC -c mychannel -ccn basic -ccp ../asset-transfer-basic/chaincode-go -ccl go

(in test network)
export PATH=${PWD}/../bin:$PATH
export FABRIC_CFG_PATH=${PWD}/../config/
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_LOCALMSPID="Org1MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=${PWD}/../organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt
export CORE_PEER_MSPCONFIGPATH=${PWD}/../organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp
export CORE_PEER_ADDRESS=localhost:7051

peer version



I am trying to setup simple blockchain network with hyperledger:

Here is what I have done:
1) Installed prerequisites 
https://hyperledger-fabric.readthedocs.io/en/release-2.5/prereqs.html

2) Got download script:
curl -sSLO https://raw.githubusercontent.com/hyperledger/fabric/main/scripts/install-fabric.sh && chmod +x install-fabric.sh

3) Executed download script:
./install-fabric.sh docker samples binary
 
4) Go to test network
cd fabric-samples/test-network

5) Set vars
export PATH=${PWD}/../bin:$PATH
export FABRIC_CFG_PATH=${PWD}/../config/
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_LOCALMSPID="Org1MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=${PWD}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt
export CORE_PEER_MSPCONFIGPATH=${PWD}/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp
export CORE_PEER_ADDRESS=localhost:7051

6) Create channel
#cryptogen generate --config=./organizations/cryptogen/crypto-config-org1.yaml --output=../organizations
./network.sh createChannel -c mychannel (./network.sh down)
./network.sh deployCC -c mychannel -ccn basic -ccp ../asset-transfer-basic/chaincode-go -ccl go
peer channel list

export FABRIC_CFG_PATH=${PWD}/configtx



git clone https://github.com/hyperledger/fabric-sdk-py.git

conda create -n fabric-env python=3.8
conda activate fabric-env
pip install fabric-sdk-py
pip install protobuf==3.20.3



7) Add some basic data in
peer chaincode invoke -o localhost:7050 --ordererTLSHostnameOverride orderer.example.com --tls --cafile ${PWD}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem -C mychannel -n basic --peerAddresses localhost:7051 --tlsRootCertFiles ${CORE_PEER_TLS_ROOTCERT_FILE} --isInit -c '{"Args":["InitLedger"]}'


Can you help me continue? Use the internet for most up to date results



