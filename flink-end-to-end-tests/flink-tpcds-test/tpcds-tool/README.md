# TPC-DS tool

This directory contains the necessary tools to run a [TPC-DS]( http://www.tpc.org/tpcds) benchmark.

## data_generator.sh scripts

Scripts to download the TPC-DS data generator and generate test data, the supported OS including GNU/Linux OS and Mac OS.
```
 data_generator.sh <generatorDir> <scaleFactor> <outputDataDir> <commonScriptsDir>
```
* generator_dir: directory to save TPC-DS data generator.
* scale_factor: scale factor indicates raw data size in GB, we use the validation scale factor 1 (1G) by default.
* data_dir: directory to save TPC-DS test data.
* common_scripts_dir: directory which contains some common scripts for Flink end to end tests, the retry scripts `retry_times_with_backoff_and_cleanup()` will be used when download the TPC-DS data generator from the Internet.

The scripts is internally used in Flink end to end tests, give an example to specific how the scripts works:
```
$ cd <your_workspace_dir>/flink                                                                                   # go to the Flink project
$ mvn compile -DskipTests=true                                                                                   # compile the Flink source code
$ export FLINK_DIR=<your_workspace_dir>/flink/flink-dist/target/flink-${flink-version}-bin/flink-${flink-version}      # set Flink distribution directory, the ${flink-version} is the compiled Flink version
$ export END_TO_END_DIR=<your_workspace_dir>/flink/flink-end-to-end-tests                                          # set end to end tests directory
$ mkdir -p <your_workspace_dir>/dir_to_save_genarator
$ mkdir -p <your_workspace_dir>/dir_to_save_data
$ cd <your_workspace_dir>/flink/flink-end-to-end-tests/tpcds-tool
$ sh data_generator.sh <your_workspace_dir>/dir_to_save_genarator 1 <your_workspace_dir>/dir_to_save_data <your_workspace_dir>/flink/flink-end-to-end-tests/test-scripts
```
The downloaded generator will be saved to `<your_workspace_dir>/dir_to_save_genarator`, the generated data will be saved to `<your_workspace_dir>/dir_to_save_data`.

**NOTE:** If you want to run a TPC-DS benchmark in Flink, please read `flink-tpcds-test/README.md` for more information.
    
## query directory

Directory that contains all TPC-DS queries (103 queries in total), the corresponding data scale factor is 1G, these queries come from TPC-DS standard specification v2.11.0 [1].

**NOTE:** The scale factor of queries should match with test data's scale factor.
    
## answer_set directory

Directory that contains all answers for per query, the answer content of query may has different order according to the system's null order.

* (1) Flink SQL keeps null first in ASC order, keeps null last in DESC order, we choose corresponding answer file when compare the answer.
* (2) For query 8, 14a, 18, 70 and 77, the decimal precision of answer set is too low, we compare query result with SQL server's answer, they can strictly match.
   
 ## Reference   
[1] http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v2.11.0.pdf
