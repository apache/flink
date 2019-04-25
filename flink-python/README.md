# Apache Flink Python API

Apache Flink is an open source stream processing framework with the powerful stream- and batch-processing capabilities.

Learn more about Flink at [http://flink.apache.org/](http://flink.apache.org/)

This packaging allows you to write Flink programs in Python, but it is currently a very initial version and will change in future versions.

In this initial version only Table API is supported, you can find the documentation at [https://ci.apache.org/projects/flink/flink-docs-master/dev/table/tableApi.html](https://ci.apache.org/projects/flink/flink-docs-master/dev/table/tableApi.html)

## Installation

Currently, you can install PyFlink from Flink source code. 
First, you need build the whole Flink project using `mvn clean install -DskipTests` and set the value of the environment variable FLINK_HOME to the `build-target` directory under the root directory of Flink.
Then enter the directory where this README.md file is located and execute `python setup.py install` to install PyFlink on your device.

## Running Tests

Currently you can perform an end-to-end test of PyFlink in the directory where this file is located with the following command:

    PYTHONPATH=$PYTHONPATH:./ python ./pyflink/table/tests/test_end_to_end.py

## Python Requirements

PyFlink depends on Py4J (currently version 0.10.8.1).
