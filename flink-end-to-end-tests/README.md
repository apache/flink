# Flink End-to-End Tests

This module contains tests that verify end-to-end behaviour of Flink.

## Running Tests
You can run all tests by executing

```
$ FLINK_DIR=<flink dir> flink-end-to-end-tests/run-pre-commit-tests.sh
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
