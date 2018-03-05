# jepsen.flink

A Clojure project based on the [Jepsen](https://github.com/jepsen-io/jepsen) framework to find bugs in the
distributed coordination of Apache Flink®.

## Test Coverage
Jepsen is a framework built to test the behavior of distributed systems
under faults. The tests in this particular project deploy Flink on either YARN or Mesos, submit a
job, and examine the availability of the job after injecting faults.
A job is said to be available if all the tasks of the job are running.
The faults that can be currently introduced to the Flink cluster include:
* Killing of TaskManager/JobManager processes
* Stopping HDFS namenode
* Network partitions

There are many more properties other than job availability that could be
verified but are not yet covered by this test suite, e.g., end-to-end exactly-once processing
semantics.

## Usage
See the [Jepsen documentation](https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment)
for how to set up the environment to run tests. The `scripts/run-tests.sh` documents how to invoke
tests. The Flink job used for testing is located under
`flink-end-to-end-tests/flink-datastream-allround-test`. You have to build the job first and copy
the resulting jar (`DataStreamAllroundTestProgram.jar`) to the `./bin` directory of this project's
root.

To simplify development, we have prepared Dockerfiles and a Docker Compose template
so that you can run the tests locally in containers. To build the images
and start the containers, simply run:

    $ cd docker
    $ ./up.sh

After the containers started, open a new terminal window and run `docker exec -it jepsen-control bash`.
This will allow you to run arbitrary commands on the control node.
To start the tests, you can use the `run-tests.sh` script in the `docker` directory,
which expects the number of test iterations, and a URI to a Flink distribution, e.g.,

    ./docker/run-tests.sh 1 https://example.com/flink-dist.tgz

The project's root is mounted as a volume to all containers under the path `/jepsen`.
This means that changes to the test sources are immediately reflected in the control node container.
Moreover, this allows you to test locally built Flink distributions by copying the tarball to the
project's root and passing a URI with the `file://` scheme to the `run-tests.sh` script, e.g.,
`file:///jepsen/flink-dist.tgz`.

### Checking the output of tests

Consult the `jepsen.log` file for the particular test run in the `store` folder. The final output of every test will be either

    Everything looks good! ヽ('ー`)ノ

or

    Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻

depending on whether the test passed or not.

In addition, the test directories contain all relevant log files aggregated from all hosts.
