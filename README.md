# mobility-dpu

**Mobility DPU** aggregates the raw activity and location sensor data collected by phones (iOS or Android) and generates more meaningful *activity episodes* and *daily mobility summary* data.

# Workflow
    * Query activity and location data streams from MongoDB.
    * Break activity data stream into segments, each of which is a sequence of continuous activity samples. 
    * Apply a Hidden Markov Chain model to smooth activity sequence.
        * The states are the true but unknown user activities the observations are the raw sensor data.
    * Merge a sequence of consecutive samples with the same inferred activity into activity episodes.
    * Merge activity episodes with location data based on time.
    * Calculate daily summaries.
 
               

## Usage

* The DPU generates and saves new data points every 10 minutes

```bash
    $ java -jar mobility-dpu-0.1.0-standalone.jar
```
