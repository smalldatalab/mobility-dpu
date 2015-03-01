# mobility-dpu

**Mobility DPU** aggregates the raw activity and location sensor data from collected by phones (iOS or Android) and generate more meaningful activity episodes and daily mobility summary.

# Workflow
    * Query activity and location data streams from MongoDB
    * Break activity data streams into segments
    * Apply a Hidden Markov Chain model to smooth activity sequence
    * Divide segments into activity episodes, each of which is a sequence of consecutive samples with the same inferred activity
    * Merge activity episodes with location data based on time
    * Calculate daily summaries
 
               

## Usage

* The DPU generates and saves new data points every 10 minutes
```bash
    $ java -jar mobility-dpu-0.1.0-standalone.jar
```
