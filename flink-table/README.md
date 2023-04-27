# Table API & SQL

Apache Flink features two relational APIs - the Table API and SQL - for unified stream and batch processing. 
The Table API is a language-integrated query API for Java, Scala, and Python that allows the composition of queries from relational operators such as selection, filter, and join in a very intuitive way.

This documentation is intended for contributors of the table modules, and not for users. 
If you want to use Table API & SQL, check out the [documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/overview/).

## Modules

### Common

* `flink-table-common`:
  * Type system definition
  * UDF stack and built-in function definitions
  * Internal data definitions
  * Extension points for catalogs, formats, connectors
  * Core APIs for extension points such as `Schema`
  * Utilities to deal with type system, internal data types and printing

### API

* `flink-table-api-java`: 
  * Java APIs for Table API and SQL
  * Package `org.apache.flink.table.delegation`, which serves as entrypoint for all planner capabilities
* `flink-table-api-scala`: Scala APIs for Table API and SQL
* `flink-table-api-bridge-base`: Base classes for APIs to bridge between Table API and DataStream API
* `flink-table-api-java-bridge`: 
  * Java APIs to bridge between Table API and DataStream API
  * Connectors that are developed using DataStream API, usually need to depend only on this module.
* `flink-table-api-scala-bridge`: Scala APIs to bridge between Table API and DataStream API
* `flink-table-api-java-uber`: 
  * Uber JAR bundling `flink-table-common` and all the Java API modules, including the bridging to DataStream API and 3rd party dependencies.
  * This module is intended to be used by the flink-dist, rather than from the users directly.
* `flink-table-calcite-brdge`: Calcite dependencies for writing planner plugins (e.g. SQL dialects) that interact with Calcite API

### Runtime

* `flink-table-code-splitter`: Tool to split generated Java code so that each method does not exceed the limit of 64KB.
* `flink-table-runtime`:
  * Operator implementations
  * Built-in functions implementations
  * Type system implementation, including readers/writers, converters and utilities
  * Raw format
  * The produced jar includes all the classes from this module and `flink-table-code-splitter`, including 3rd party dependencies

### Parser and planner

* `flink-sql-parser`: Default ANSI SQL parser implementation
* `flink-table-planner`:
  * AST and Semantic tree
  * SQL validator
  * Query planner, optimizer and rules implementation
  * Code generator
  * The produced jar includes all the classes from this module together with the two parsers, including 3rd party dependencies (excluding Scala dependencies).
* `flink-table-planner-loader-bundle` Bundles `flink-table-planner`, including Scala dependencies.
* `flink-table-planner-loader`: Loader for `flink-table-planner` that loads the planner and it's Scala dependencies in a separate classpath using `flink-table-planner-loader-bundle`, isolating the Scala version used to compile the planner.

### SQL client

* `flink-sql-client`: CLI tool to submit queries to a Flink cluster

### Testing

* `flink-table-test-utils`: Brings in transitively all the dependencies you need to execute Table pipelines and provides some test utilities such as assertions, mocks and test harnesses.

### Notes

No module except `flink-table-planner` should depend on `flink-table-runtime` in production classpath,
no module except `flink-table-planner-loader` should depend on `flink-table-planner-loader-bundle` in production classpath,
and similarly no module should depend on `flink-table-planner` or `flink-table-planner-loader` in production classpath.
For testing, you should depend on `flink-table-planner-loader` and `flink-table-runtime`.
These are already shipped by the Flink distribution.
