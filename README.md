## Description
* This DPU module is designed to work along side the ohmage-omh DSU and omh-shims server. It periodically perform the following tasks:
    * Generates mobility-data-summary mobility-data-segments from raw data from Mobility App and Moves App
    * Fetch the data from shims-server and write the normalized data to the DSU. Please specify the service you want to synchronize in the *sync-tasks* field in the *config.json*
               
## Compile
* Install [Leiningen 2.+](http://leiningen.org/)
* Compile standalone jar file with the bash command: 

```bash 
      $> lein uberjar
```

## Usage
* Config it with the following environment variables
```bash
   export MONGODB_URI=mongodb uri e.g. mongodb://127.0.0.1:27017/test
   export LOG_FILE=/path/to/log/file
   export GMAP_GEO_CODING_SERVER_KEY=google map server key
   export SHIM_ENDPOINT=url of the shim server* e.g. http://localhost:8083
   export SYNC_TASKS=data types to sync in provider:DATA_TYPE, ... format e.g. fitbit:STEPS,fitbit:ACTIVITY
```
* Use the following command to start the DPU.
```bash
    $> java -jar mobility-dpu-0.3.0-standalone.jar
```

### Mobility Data Processing Procedure (for developer only)
* Query Mobility raw activity and location data streams from MongoDB.
* Break activity data stream into segments, each of which is a sequence of continuous activity samples. 
* Apply a Hidden Markov Chain model to smooth activity sequence.
    * The states are the true but unknown user activities the observations are the raw sensor data.
* Merge a sequence of consecutive samples with the same inferred activity into activity episodes.
* Merge activity episodes with location data based on time.
* Calculate daily summaries.
