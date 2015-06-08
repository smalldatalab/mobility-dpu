## Description
* This DPU module is designed to work along side the ohmage-omh DSU and omh-shims server. It periodically perform the following tasks:
    * Generates mobility-data-summary mobility-data-segments from raw data from Mobility App and Moves App
    * Fetch the data from shims-server and write the normalized data to the DSU. Please specify the service you want to sychronize in the *sync-tasks* field in the *config.json*  
               
## Compile
* Install [Leiningen 2.+](http://leiningen.org/)
* Compile standalone jar file with the bash command: 

```bash 
      $> lein uberjar
```

## Usage
* Create a *config.json* in the running context's current directory and specify the configuration. If the *config.json* is not found, the DPU will use the default configuration (see [config.json](https://github.com/smalldatalab/mobility-dpu/blob/master/config.json)).
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
