# Apache Flink Python API

Apache Flink is an open source stream processing framework with the powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

This packaging allows you to write Flink programs in Python, but it is currently a very initial version and will change in future versions.

In this initial version only Table API is supported, you can find the documentation at [https://ci.apache.org/projects/flink/flink-docs-master/dev/table/tableApi.html](https://ci.apache.org/projects/flink/flink-docs-master/dev/table/tableApi.html)

## Installation

Currently, we can install PyFlink from Flink source code. Enter the directory where this README.md file is located and install PyFlink on your device by executing 

```
python setup.py install
```

## Running test cases 

Currently, we use conda and tox to verify the compatibility of the Flink Python API for multiple versions of Python and will integrate some useful plugins with tox, such as flake8.
We can enter the directory where this README.md file is located and run test cases by executing

```
./dev/lint-python.sh
```

## Python Requirements

PyFlink depends on Py4J (currently version 0.10.8.1).
