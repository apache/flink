# flink-jepsen

A Clojure project based on the [Jepsen](https://github.com/jepsen-io/jepsen) framework to find bugs in the
distributed coordination of Apache Flink®.

## Test Coverage
Jepsen is a framework built to test the behavior of distributed systems
under faults. The tests in this particular project deploy Flink on YARN, Mesos, or as a standalone cluster, submit a
job, and examine the availability of the job after injecting faults.
A job is said to be available if all the tasks of the job are running.
The faults that can be currently introduced to the Flink cluster include:

* Killing of TaskManager/JobManager processes
* Stopping HDFS NameNode
* Network partitions

There are many more properties other than job availability that could be
verified but are not yet fully covered by this project, e.g., end-to-end exactly-once processing
semantics.

## Usage

### Setting up the Environment
See the [Jepsen documentation](https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment)
for details on how to set up the environment required to run the tests.
To simplify development, we have prepared Dockerfiles and a [Docker Compose](https://docs.docker.com/compose/) template
so that you can run the tests locally in containers (see Section [Docker](#usage-docker)).

### Running Tests
This project does not comprise of only a single test that can be run but rather a parameterizable
test template. This allows the user to specify the cluster manager that Flink should be on, the
location of the high availability storage directory, the jobs to be submitted, etc.
The script under `docker/run-tests.sh` shows examples on how to specify and run tests.
By default, the example tests run the `DataStreamAllroundTestProgram`, which is located under
`flink-end-to-end-tests/flink-datastream-allround-test` of the Flink project root.
Before running the tests, you have to build the job first, and copy the resulting jar
(`DataStreamAllroundTestProgram.jar`) to the `./bin` directory of this project's root.
Also included in the examples is a more complicated scenario with two jobs that share a Kafka
topic. See the `run-tests.sh` script for details on how to enable and run this test.

### Docker
To build the images and start the containers, simply run:

    $ cd docker
    $ ./up.sh

This should start one control node container and three containers that will be used as DB nodes.
After the containers have started, open a new terminal window and run `docker exec -it jepsen-control bash`.
This will allow you to run arbitrary commands on the control node.
To start the tests, you can use the `run-tests.sh` script in the `docker` directory,
which expects the number of test iterations, and a URI to a Flink distribution, e.g.,

    ./docker/run-tests.sh 1 https://example.com/flink-dist.tgz

The project's root is mounted as a volume to all containers under the path `/jepsen`.
This means that changes to the test sources are immediately reflected in the control node container.
Moreover, this allows you to test locally built Flink distributions by copying the tarball to the
project's root and passing a URI with the `file://` scheme to the `run-tests.sh` script, e.g.,
`file:///jepsen/flink-dist.tgz`.

#### Memory Requirements

The tests have high memory demands due to the many processes that are started by the control node.
For example, to test Flink on YARN in a HA setup, we require ZooKeeper, HDFS NameNode,
HDFS DataNode, YARN NodeManager, and YARN ResourceManager, in addition to the Flink processes.
We found that the tests can be run comfortably in Docker containers on a machine with 32 GiB RAM. 

### Checking the Output of Tests

Consult the `jepsen.log` file for the particular test run in the `store` folder. The final output of every test will be either

    Everything looks good! ヽ('ー`)ノ

or

    Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻

depending on whether the test passed or not. If neither output is generated, the test did not finish
properly due to problems of the environment, bugs in Jepsen or in the test suite, etc.

In addition, the test directories contain all relevant log files, and the jstack output for all Flink JVMs
aggregated from the DB nodes.
