# Table Planner

This module connects Table/SQL API and runtime. It is responsible for translating and optimizing a table program into a Flink pipeline. 
For user documentation, check the [table documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/overview/).

This README contains some development info for table planner contributors.

## Json Plan unit tests

Unit tests verifying the JSON plan changes (e.g. Java tests in `org.apache.flink.table.planner.plan.nodes.exec.stream`) 
can regenerate all the files setting the environment variable `PLAN_TEST_FORCE_OVERWRITE=true`.
