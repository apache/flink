# Table Planner

This module connects Table/SQL API and runtime. It is responsible for translating and optimizing a table program into a Flink pipeline. 
For user documentation, check the [table documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/overview/).

This README contains some development info for table planner contributors.

## Immutables for rules

Since Flink 1.17.0 instead of Calcite's `@ImmutableBeans.Property`, removed in Calcite 1.28.0, 
for config methods there is [Immutables](http://immutables.github.io/) to generate immutable 
classes for rule configs based on provided interface. The configs should be annotated with `@Value.Immutable`. 
In case of config is a nested class the enclosing one should be annotated with `@Value.Enclosing`. 
Once a new rule config is written with use of Immutables the module compilation should be done to generate Immutable class for that config. 
Generated code will be placed in `target/generated-sources/annotations`. 
Then config could be built with help of generated immutables class having a name `Immutable<EnclosingClassName>.<ConfigClassName>`.
In case of issues please double-check if a required generated class is present.
Also, as an example have a look at `org.apache.flink.table.planner.plan.rules.logical.EventTimeTemporalJoinRewriteRule`.

## Json Plan unit tests

Unit tests verifying the JSON plan changes (e.g. Java tests in `org.apache.flink.table.planner.plan.nodes.exec.stream`) 
can regenerate all the files setting the environment variable `PLAN_TEST_FORCE_OVERWRITE=true`.
