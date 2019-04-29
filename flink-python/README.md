# Apache Flink Python API

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [http://flink.apache.org/](http://flink.apache.org/)

This packaging is currently a very initial version and will change in future versions.

## Installation

In order to use PyFlink, you need to install Flink on your device and set the value of the environment variable FLINK_HOME to the root directory of Flink.
Then enter the directory where this README.md file is located and execute `python setup.py install` to install PyFlink on your device.

## Running Tests

Currently you can perform an end-to-end test of PyFlink in the directory where this file is located with the following command:

    PYTHONPATH=./ python ./pyflink/table/tests/test_end_to_end.py

## Python Requirements

PyFlink currently depends on `Py4J 0.10.8.1`.
