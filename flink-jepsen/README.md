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
A Jepsen test is started and orchestrated by a Clojure program running on a _control node_.
That program logs into several specified _DB nodes_ to set up a Flink cluster
and submit test job(s). Afterwards, the control node will inject faults into the DB nodes
and continue to interact with the Flink cluster (for example by probing the status of the test job).
Tests in this project require at least 3 DB nodes; this is due to [how we install
software](#test-spec), such as HDFS, on the DB nodes. There is no upper limit on the number of DB nodes.

For details on how to set up the environment required to run the tests,
see the [Jepsen documentation](https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment).
To simplify development, we have prepared Dockerfiles and a [Docker Compose](https://docs.docker.com/compose/) template
so that you can run the tests locally in containers (see Section [Docker](#usage-docker)).

### Running Tests
Because tests are started from the control node, the contents of this directory
must be copied to the control node first. An example for a command that will run a test is

```bash
lein run test --tarball http://             \
--ssh-private-key ~/.ssh/id_rsa             \
--nodes-file nodes                          \
--nemesis-gen kill-task-managers            \
--test-spec test-specs/yarn-session.edn
```

Here is a breakdown of the command line options and their arguments:

* `--tarball` A URL to the Flink distribution under test
* `--ssh-private-key` Path to the private key that will be used to connect to the DB nodes (from the control node)
* `--nodes-file` Path to a file containing newline separated IPs or hostnames of the DB nodes
* `--nemesis-gen` Specifies the faults that will be injected. `kill-task-managers` means for the duration of the test (default 60s),
TaskManagers will be killed on all DB nodes with a uniform random timing noise with a mean delay of 3 seconds
(delay ranging from 0 to 6 seconds).
* `--test-spec` Path to an [edn](https://github.com/edn-format/edn) formatted file
containing a specification about which software to install and which jobs to submit. For example, by using the file below,
we would request the framework to install Hadoop (YARN, HDFS), ZooKeeper, and deploy Flink as a session cluster on YARN.
Furthermore, we instruct to submit the `DataStreamAllroundTestProgram` to Flink with the specified job arguments.
  ```
  {:dbs  [:hadoop :zookeeper :flink-yarn-session]
   :jobs [{:job-jar  "/path/to/DataStreamAllroundTestProgram.jar"
           :job-args "--environment.parallelism 1 --state_backend.checkpoint_directory hdfs:///checkpoints --state_backend rocks --state_backend.rocks.incremental true"}]}
  ```

For more details on the command line options, see Section [Command Line Options & Configuration](#command-line-options--configuration).

As you can see, this project does not comprise only a single test but rather a parameterizable
test template. The script under `docker/run-tests.sh` shows more examples on how to parameterize and run tests.
The example tests can be used as a starting point for testing releases or
patches that are modifying critical components of Flink.
Most examples submit the `DataStreamAllroundTestProgram` to the Flink cluster under test.
The job must be build before running the tests, for example by running

    $ mvn clean package -pl flink-end-to-end-tests/flink-datastream-allround-test -am -DskipTests -Dfast

from Flink's project root.

Also included in the provided test suite is a more complicated scenario with two jobs that share a Kafka
topic. See the `docker/run-tests.sh` script for details on how to enable and run this test.

### Docker
Firstly, build the test job located under `flink-end-to-end-tests/flink-datastream-allround-test`
and copy the resulting jar (`DataStreamAllroundTestProgram.jar`) to the `./bin` directory of this project's root.

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

### Command Line Options & Configuration

Below is a listing of the most important command line options that can be passed
to `lein run test`.

| Command                                 | Default Value      | Description |
| :-------------------------------------- | :----------------- | :---------- |
| `-h` `--help`                           |                    | Prints the help text |
| `--nodes-file FILENAME`                 |                    | Path to a file containing newline separated IPs or hostnames of the DB nodes |
| `--ssh-private-key FILE`                |                    | Path to the private key that will be used to connect to the DB nodes (from the control node) |
| `--time-limit SECONDS`                  | 60                 | Excluding setup and teardown, how long should a test run for, in seconds? |
| `--test-spec FILE`                      |                    | Path to a test specification (.edn) |
| `--nemesis-gen GEN`                     | kill-task-managers | Which nemesis should be used? Must be one of: `kill-task-managers`, `kill-single-task-manager`, `kill-random-task-managers`, `kill-task-managers-bursts`, `kill-job-managers`, `fail-name-node-during-recovery`, `utopia` |
| `--client-gen GEN`                      | poll-job-running   | Which client should be used? Must be one of: `poll-job-running`, `cancel-jobs` |
| `--job-running-healthy-threshold TIMES` | 5                  | Number of consecutive times the job must be running to be considered healthy |
| `--job-recovery-grace-period SECONDS`   | 180                | Time period in which the job must become healthy |
| `--tarball URL`                         |                    | URL for the DB tarball to install. May be either HTTP, HTTPS, or a local file on each DB node. For instance, `--tarball https://foo.com/bar.tgz` or `file:///tmp/bar.tgz`

#### Nemesis
The nemesis or more accurately the nemesis generator determines the order and timing of the injected faults.

| Nemesis Generator              | Description |
| :----------------------------- | :---------- |
| kill-task-managers _(default)_ | Kills all TaskManagers with a uniform random timing noise with a mean delay of 3 seconds (delay ranging from 0 to 6 seconds). |
| kill-single-task-manager       | Kills all TaskManagers on a random DB node with a uniform random timing noise with a mean delay of 3 seconds (delay ranging from 0 to 6 seconds). |
| kill-random-task-managers      | Kills all TaskManagers on a random non-empty subset of all DB nodes with a uniform random timing noise with a mean delay of 3 seconds (delay ranging from 0 to 6 seconds). |
| kill-task-managers-bursts      | Kills all TaskManagers on all DB nodes 20 times in succession every 300 seconds. |
| kill-job-managers              | Waits for 60 seconds, then repeatedly kills all JobManagers without delay. |
| fail-name-node-during-recovery | Waits for 60 seconds, partitions the network into randomly chosen halves, shuts down the HDFS NameNode, waits for 20 seconds, heals the network, waits for another 60 seconds, and finally starts the HDFS NameNode again. |
| utopia                         | No faults will be injected. Useful for testing if the job runs at all. |

#### Client

The client interacts with the Flink cluster during the test; its result history will be
used to validate whether the Flink cluster behaved correctly.

| Client Generator             | Description |
| :--------------------------- | :---------- |
| poll-job-running _(default)_ | Probes whether the job is running every 5 seconds. |
| cancel-jobs                  | Same as `poll-job-running` but additionally cancels the job after 15s. |

#### Test Spec
The test spec is an [edn](https://github.com/edn-format/edn) formatted file
containing a specification about which software to install on the DB nodes and which jobs to submit.
Additionally the user can choose to override the default Flink configuration in the test spec.
Below is a complete example for a test spec file.

    {:dbs          [:hadoop :zookeeper :flink-yarn-session]
     :jobs         [{:job-jar    "/path/to/FlinkJobs.jar"
                     :job-args   "--parameter1 arg1 --parmeter2 arg2"
                     :main-class "org.apache.flink.streaming.tests.Job1"}

                     {:job-jar    "/path/to/FlinkJobs.jar"
                      :job-args   "--parameter1 arg1 --parmeter2 arg2"
                      :main-class "org.apache.flink.streaming.tests.Job2"}]

     :flink-config {:jobmanager.execution.failover-strategy "region"}}

The test spec is a Clojure map; its set of allowed keys is described by the table below.

| Key           | Description |
| :------------ | :---------- |
| :dbs          | A vector specifying which software to install on the DB nodes before running the test. Possible values: `:hadoop`, `:kafka`, `:mesos`, `:zookeeper`, `:flink-standalone-session`, `:flink-mesos-session`, `:flink-yarn-job`, `:flink-yarn-session`. |
| :jobs         | A vector of maps specifying which jobs to submit to the Flink cluster. Multiple jobs can be submitted. The `:main-class` key is optional; if omitted, the main class will be looked up in the manifest file. |
| :flink-config | A map with additional Flink configuration; has precedence over the default configuration. |

The set of possible _dbs_ is described by the table below. Note that when we speak of the _first node_, the _second node_, _nth node_,
we refer to the _first_, _second_, _nth_ item in the nodes file in lexicographically sorted order.

| DB Keyword                | Description |
| :-------------------------| :---------- |
| :hadoop                   | Installs HDFS and YARN. The first node and the second node will run the HDFS NameNode and the YARN ResourceManager, respectively. The remaining nodes will take the role of the HDFS DataNodes and YARN NodeManagers. |
| :kafka                    | Installs Kafka on all nodes. |
| :mesos                    | Installs Mesos and Marathon. The first node will run the Mesos master and Marathon. The remaining nodes will run the Mesos agent. |
| :zookeeper                | Installs ZooKeeper on all nodes. |
| :flink-mesos-session      | Starts a session cluster on Mesos via Marathon. |
| :flink-standalone-session | Starts JobManagers on the first two nodes and TaskManagers on the remaining nodes. |
| :flink-yarn-job           | Starts a per-job cluster on YARN. |
| :flink-yarn-session       | Starts a session cluster on YARN. |
