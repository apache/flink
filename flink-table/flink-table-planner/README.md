# Table Planner

This module connects Table/SQL API and runtime. It is responsible for translating and optimizing a table program into a Flink pipeline. 
For user documentation, check the [table documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/overview/).

This README contains some development info for table planner contributors.

## Immutables for rules

Calcite's `@ImmutableBeans.Property` has been removed in Calcite 1.28.0.

Since Flink 1.17, planner rules use [Immutables](http://immutables.github.io/) to generate immutable
classes for rule configs based on the provided interface. A config should be annotated with `@Value.Immutable`.
In case the config is a nested class, the enclosing one should be annotated with `@Value.Enclosing`.

Once a new rule config is written and annotated, compile the module to generate the immutable class
for that config. Generated code will be placed in `target/generated-sources/annotations`. The config
can then be instantiated with the help of the generated immutable class like `Immutable<EnclosingClassName>.<ConfigClassName>`.

In case of issues, please double-check if a required generated class is present.
As an example have a look at `org.apache.flink.table.planner.plan.rules.logical.EventTimeTemporalJoinRewriteRule`.
See also `org.apache.calcite.plan.RelRule` for detailed explanation from Calcite.

## Json Plan unit tests

Unit tests verifying the JSON plan changes (e.g. Java tests in `org.apache.flink.table.planner.plan.nodes.exec.stream`) 
can regenerate all the files setting the environment variable `PLAN_TEST_FORCE_OVERWRITE=true`.
