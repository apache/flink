# Flink [TPC-DS]( http://www.tpc.org/tpcds) test

You can run the Flink TPC-DS test in your local environment as following commands:

```
$ cd <your_workspace_dir>/flink                                                                               # go to the Flink project   
$ mvn compile -DskipTests=true                                                                               # compile Flink source code
$ export FLINK_DIR=<your_workspace_dir>/flink/flink-dist/target/flink-${flink-version}-bin/flink-${flink-version}  # set Flink distribution directory, the ${flink-version} is the compiled Flink version
$ sh flink-end-to-end-tests/run-single-test.sh flink-end-to-end-tests/test-scripts/test_tpcds.sh               # run a TPC-DS benchmark
```

The default scale factor of TPC-DS benchmark in Flink is 1GB, Flink use this scale factor from validation purpose.
