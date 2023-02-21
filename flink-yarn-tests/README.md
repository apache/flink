# Flink YARN tests

`flink-yarn-test` collects test cases which are deployed to a local Apache Hadoop YARN cluster. 
There are several things to consider when running these tests locally:

* `YarnTestBase` spins up a `MiniYARNCluster`. This cluster spawns processes outside of the IDE's JVM 
  to run the workers on. `JAVA_HOME` needs to be set to make this work.
* The Flink cluster within each test is deployed using the `flink-dist` binaries. Any changes made 
  to the code will only take effect after rebuilding the `flink-dist` module.
* Each `YARN*ITCase` will have a local working directory for resources like logs to be stored. These 
  working directories are located in `flink-yarn-tests/target/` (see 
  `find flink-yarn-tests/target -name "*.err" -or -name "*.out"` for the test's output).
