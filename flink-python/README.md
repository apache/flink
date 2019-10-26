# Apache Flink

Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams. Flink has been designed to run in all common cluster environments, perform computations at in-memory speed and at any scale.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Python Packaging

This packaging allows you to write Flink programs in Python, but it is currently a very initial version and will change in future versions.

In this initial version only Table API is supported, you can find the documentation at [https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/tableApi.html](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/tableApi.html)

The tutorial can be found at [https://ci.apache.org/projects/flink/flink-docs-stable/tutorials/python_table_api.html](https://ci.apache.org/projects/flink/flink-docs-stable/tutorials/python_table_api.html)

The auto-generated Python docs can be found at [https://ci.apache.org/projects/flink/flink-docs-stable/api/python/](https://ci.apache.org/projects/flink/flink-docs-stable/api/python/)

## Python Requirements

Apache Flink Python API depends on Py4J (currently version 0.10.8.1), CloudPickle (currently version 1.2.2), python-dateutil(currently version 2.8.0) and Apache Beam (currently version 2.15.0).

## Development Notices

### Protobuf Code Generation

Protocol buffer is used in file `flink_fn_execution_pb2.py` and the file is generated from `flink-fn-execution.proto`. Whenever `flink-fn-execution.proto` is updated, please re-generate `flink_fn_execution_pb2.py` by executing:

```
python pyflink/gen_protos.py
```

PyFlink depends on the following libraries to execute the above script:
1. grpcio-tools (>=1.3.5,<=1.14.2)
2. setuptools (>=37.0.0)
3. pip (>=7.1.0)

### Running Test Cases 

Currently, we use conda and tox to verify the compatibility of the Flink Python API for multiple versions of Python and will integrate some useful plugins with tox, such as flake8.
We can enter the directory where this README.md file is located and run test cases by executing

```
./dev/lint-python.sh
```
