# Apache Flink

Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams. Flink has been designed to run in all common cluster environments, perform computations at in-memory speed and at any scale.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Python Packaging

PyFlink is a Python API for Apache Flink that allows you to build scalable batch and streaming workloads,
such as real-time data processing pipelines, large-scale exploratory data analysis, Machine Learning (ML)
pipelines and ETL processes. If you’re already familiar with Python and libraries such as Pandas,
then PyFlink makes it simpler to leverage the full capabilities of the Flink ecosystem.
Depending on the level of abstraction you need, there are two different APIs that can be used in PyFlink: PyFlink Table API and PyFlink DataStream API.

The PyFlink Table API allows you to write powerful relational queries in a way that is similar to
using SQL or working with tabular data in Python. You can find more information about it via the tutorial
[https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table_api_tutorial/](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table_api_tutorial/)

The PyFlink DataStream API gives you lower-level control over the core building blocks of Flink,
state and time, to build more complex stream processing use cases.
Tutorial can be found at [https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/datastream_tutorial/](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/datastream_tutorial/)

You can find more information via the documentation at [https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/overview/](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/overview/)

The auto-generated Python docs can be found at [https://nightlies.apache.org/flink/flink-docs-stable/api/python/](https://nightlies.apache.org/flink/flink-docs-stable/api/python/)

## Python Requirements

Apache Flink Python API depends on Py4J (currently version 0.10.9.7), CloudPickle (currently version 2.2.0), python-dateutil (currently version >=2.8.0,<3), Apache Beam (currently version >=2.43.0,<2.49.0).

## Development Notices

### Protobuf Code Generation

Protocol buffer is used in file `flink_fn_execution_pb2.py` and `flink_fn_execution_pb2.pyi`, the file is generated from `flink-fn-execution.proto`. Whenever `flink-fn-execution.proto` is updated, please re-generate `flink_fn_execution_pb2.py` and `flink_fn_execution_pb2.pyi` by executing:

```
python pyflink/gen_protos.py
```

PyFlink depends on the following libraries to execute the above script:
1. grpcio-tools (>=1.29.0,<=1.50.0)
2. setuptools (>=37.0.0)
3. pip (>=20.3)

### Running Test Cases 

Currently, we use conda and tox to verify the compatibility of the Flink Python API for multiple versions of Python and will integrate some useful plugins with tox, such as flake8.
We can enter the directory where this README.md file is located and run test cases by executing

```
./dev/lint-python.sh
```

To use your system conda environment, you can set `FLINK_CONDA_HOME` variable:

```shell
export FLINK_CONDA_HOME=$(dirname $(dirname $CONDA_EXE))
```

Create a virtual environment:
```shell
conda create -n pyflink_38 python=3.8
```

Then you can activate your environment and run tests, for example:

```shell
conda activate pyflink_38
pip install -r ./dev/dev-requirements.txt
./dev/lint-python.sh
```
