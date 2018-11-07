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

where <flink dir> is a Flink distribution directory.

You can also run tests individually via

```
$ FLINK_DIR=<flink dir> flink-end-to-end-tests/run-single-test.sh your_test.sh arg1 arg2
```

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
- Reverting config to default (if changed before)
- Cleaning up log and temp directories

In some cases your test is required to do to some *additional* cleanup, for example shutting down external systems like Kafka or Elasticsearch. In this case it is a common pattern to trap a `test_cleanup` function to `EXIT` like this:

```sh
function test_cleanup {
    # do your custom cleanup here
}

trap test_cleanup EXIT
```
