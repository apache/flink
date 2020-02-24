# Apache Flink

Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams. Flink has been designed to run in all common cluster environments, perform computations at in-memory speed and at any scale.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Python Packaging

This packaging allows you to write Flink programs in Python, but it is currently a very initial version and will change in future versions.

In this initial version only Table API is supported, you can find the documentation at [https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/tableApi.html](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/tableApi.html)

The tutorial can be found at [https://ci.apache.org/projects/flink/flink-docs-release-1.9/tutorials/python_table_api.html](https://ci.apache.org/projects/flink/flink-docs-release-1.9/tutorials/python_table_api.html)

The auto-generated Python docs can be found at [https://ci.apache.org/projects/flink/flink-docs-release-1.9/api/python/](https://ci.apache.org/projects/flink/flink-docs-release-1.9/api/python/)

## Python Requirements

Apache Flink Python API depends on Py4J (currently version 0.10.8.1) and python-dateutil (latest version is recommended).
