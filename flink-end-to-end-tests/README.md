# Flink End-to-End Tests

This module contains tests that verify end-to-end behaviour of Flink. We
categorize end-to-end tests as either pre-commit tests or nightly tests. The
former should be run on every commit, that is every Travis run, while the second
category should be run by a nightly job or when manually verifying a release or
making sure that the tests pass.

Tests in the pre-commit category should be more lightweight while tests in the
nightly category can be quite heavyweight because we don't run them for every
commit.

## Running Tests
You can run all pre-commit tests by executing

```
$ FLINK_DIR=<flink dir> flink-end-to-end-tests/run-pre-commit-tests.sh
```

and all nightly tests via

```
$ FLINK_DIR=<flink dir> flink-end-to-end-tests/run-nightly-tests.sh
```

where \<flink dir\> is a Flink distribution directory.

You can also run tests individually via

```
$ FLINK_DIR=<flink dir> flink-end-to-end-tests/run-single-test.sh your_test.sh arg1 arg2
```

**NOTICE**: Please _DON'T_ run the scripts with explicit command like ```sh run-nightly-tests.sh``` since ```#!/usr/bin/env bash``` is specified as the header of the scripts to assure flexibility on different systems.

### Streaming bucketing test

Before running this nightly test case (test_streaming_bucketing.sh), please make sure to run `mvn -DskipTests install` in the `flink-end-to-end-tests` directory, so jar files necessary for the test like `BucketingSinkTestProgram.jar` could be generated.

What's more, starting from 1.8.0 release it's required to make sure that `HADOOP_CLASSPATH` is [correctly set](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/hadoop.html) or the pre-bundled hadoop jar has been put into the `lib` folder of the `FLINK_DIR` (You can find the binaries from the [Downloads page](https://flink.apache.org/downloads.html) on the Flink project site).

### Kubernetes test

Kubernetes test (test_kubernetes_embedded_job.sh) assumes a running minikube cluster.

## Writing Tests

### Examples
Have a look at `test_batch_wordcount.sh` for a very basic test and
`test_streaming_kafka010.sh` for a more involved example. Whenever possible, try
to put new functionality in `common.sh` so that it can be reused by other tests.

### Adding a test case
In order to add a new test case you need add it to either `test-scripts/run-nightly-tests.sh` and / or `test-scripts/run-pre-commit-tests.sh`. Templates on how to add tests can be found in those respective files.

_Note: If you want to parameterize your tests please do so by adding multiple test cases with parameters as arguments to the nightly / pre-commit test suites. This allows the test runner to do a cleanup in between each individual test and also to fail those tests individually._

_Note: While developing a new test case make sure to enable bash's error handling in `test-scripts/common.sh` by uncommenting `set -Eexuo pipefail` and commenting the current default `set` call. Once your test is implemented properly, add `set -Eeuo pipefail` on the very top of your test script (before any `common` script)._

### Passing your test
A test is considered to have passed if it:
- has exit code 0
- there are no non-empty .out files (nothing was written to stdout / stderr by your Flink program)
- there are no exceptions in the log files
- there are no errors in the log files

_Note: There is a whitelist for exceptions and errors that do not lead to failure, which can be found in the `check_logs_for_errors` and `check_logs_for_exceptions` in `test-scripts/common.sh`._

Please note that a previously supported pattern where you could assign a value the global variable `PASS` to have your tests fail **is not supported anymore**.

### Cleanup
The test runner performs a cleanup after each test case, which includes:
- Stopping the cluster
- Killing all task and job managers
- Reverting `conf` and `lib` dirs to default
- Cleaning up log and temp directories

In some cases your test is required to do some *additional* cleanup, for example shutting down external systems like Kafka or Elasticsearch. In this case you can register a function that will be called on test exit like this:

```sh
function test_cleanup {
    # do your custom cleanup here
}

on_exit test_cleanup
```
