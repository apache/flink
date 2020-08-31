# TPC-DS tool

This directory contains the necessary scripts/files to run a TPC-DS test.

## data_generator.sh scripts

Scripts that to download the TPC-DS data generator and generate test data, the supported OS including GNU/Linux OS and Mac OS.

* parameter 1: generator_dir, the directory to save TPC-DS data generator.
* parameter 2: scale_factor, the TPC-DS test scale factor, we use the validation scale factor 1 (1G) by default.
* parameter 3: data_dir, the directory to save TPC-DS test data.
* parameter 4: retry_scripts_dir, the directory which contains some common scripts for end-to end tests, the retry scripts will be used when download the TPC-DS data generator.  
    
## query directory

Directory that contains all TPC-DS queries (103 queries in total), the corresponding data scale factor is 1G, these queries comes from TPC-DS standard specification v2.11.0 [1].

*Note:* The scale factor of queries should match with test data's scale factor.
    
## answer_set directory

Directory that contains all answers for per query, the answer content of query may has different order according to the system's null order.

* (1) Flink SQL keeps null first in ASC order, keeps null last in DESC order, we choose corresponding answer file when compare the answer.
* (2) For query 8, 14a, 18, 70 and 77, the decimal precision of answer set is too low and unreasonable, we compare result with result from SQL server, they can strictly match.
   
 ## Reference   
[1] http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v2.11.0.pdf