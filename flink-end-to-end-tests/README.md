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
$ FLINK_DIR=<flink dir> flink-end-to-end-tests/test-scripts/test_batch_wordcount.sh
```

## Writing Tests

Have a look at test_batch_wordcount.sh for a very basic test and
test_streaming_kafka010.sh for a more involved example. Whenever possible, try
to put new functionality in common.sh so that it can be reused by other tests.
