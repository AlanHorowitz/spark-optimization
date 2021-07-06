## Spark Optimization Mini Project

### Improve upon an aggregated query joining two parquet files.

### Demonstration instructions

- See before and after programs in optimize.py

### Results

1. The only material performance improvement I could measure on my stand-alone spark cluster was
   to reduce the number of shuffle partitions to 4.
2. I also filtered for the needed fields immediately, but could not identify any benefits in the consequent plans.

3. I also experimented with the following scenarios:
   - Join before aggregation (in the hope that Spark would realize efficiencies my doing the and order by on a single
   table)
   - Repartition the answer dataframe by question_id or question_id and month.
   - Load the parquet files as tables to see if they had any metadata to leverage.


See _explore.ipynb_ to see the experiments. 
