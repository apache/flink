<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# SQL Script for testing SQL Client

This is a guideline about how to add SQL Script for testing SQL Client.

## An Example

Write the following script, and save it as `src/test/resources/sql/emp.q`:

```
SET execution.result-mode = tableau;
!info

SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi')) as T(id, str);
!ok
```

Run `org.apache.flink.table.client.cli.CliClientITCase` using IDE or using maven's surefire plugin.
There should be JUnit comparison failure for the `emp.q`. The printed actual result should be:

```
SET execution.result-mode = tableau;
[INFO] Session property has been set.
!info

SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi')) as T(id, str);
+-------------+----------------------+
|          id |                  str |
+-------------+----------------------+
|           1 |          Hello World |
|           2 |                   Hi |
|           2 |                   Hi |
+-------------+----------------------+
Received a total of 3 rows
!ok
```

Output is mixed in with input. The output of each statement actually occurs just *before* the command:

- The `!info` command executes the statement and checks the printed output is as expected and is formatted using information attribute (bold and blue color).
- The `!ok` command executes the statement and checks printed output is as expected and is not formatted using any attribute.

This output looks good, so we can just copy the actual result into `src/test/resources/sql/emp.q`.

Run the script again, and sure enough, it passes.

We can easily make incremental changes to a script, such as adding comments, adding statements,
re-ordering statements. You only need to run the script again, compare result, and copy result.

For more examples, look at the `xx.q` files in the `flink-table/flink-sql-client/src/test/resources/sql/` project path.


## Run

Run `org.apache.flink.table.client.cli.CliClientITCase` using IDE or using maven's surefire plugin.

## Script commands

### `# a comment line`

Comments are printed and not executed. The line must start with a `#`.

### `<sql statement>;`

Sets the current query to `<sql statement>` (`SELECT`, `INSERT`, etc.) Queries may span multiple lines, and must be terminated with a semi-colon, `';'`.

### `!error`

Executes the current query and checks that it returns a particular error.

The command succeeds if the expected error is non-empty and exactly the same with the printed error message
and also checks the printed error message is formatted with bold and red color.

Spaces and line endings do not need to match exactly.

Example:

```
# Match the error message
use catalog non_existing_catalog;
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.catalog.exceptions.CatalogException: A catalog with name [non_existing_catalog] does not exist.
!error
```

### `!warning`

Executes the current statement and prints the formatted output with information attribute (bold and yellow color).

Example:

```
# test set the removed key
SET execution.max-idle-state-retention=1000;
[WARNING] The specified key is not supported anymore.
!warning
```

### `!info`

Executes the current statement and prints the formatted output with information attribute (bold and blue color).

Example:

```
CREATE TABLE IF NOT EXISTS orders (
 `user` BIGINT NOT NULl,
 product VARCHAR(32),
 amount INT,
 ts TIMESTAMP(3),
 ptime AS PROCTIME(),
 PRIMARY KEY(`user`) NOT ENFORCED,
 WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS
) with (
 'connector' = 'datagen'
);
[INFO] Table has been created.
!info
```

### `!ok`

Executes the current statement and prints the output, the output is not formatted with any attribute (bold or color).
The output appears before the `!ok` line.

Example:

```
SELECT * FROM (VALUES (1, 'Hello World'), (2, 'Hi'), (2, 'Hi')) as T(id, str);
+-------------+----------------------+
|          id |                  str |
+-------------+----------------------+
|           1 |          Hello World |
|           2 |                   Hi |
|           2 |                   Hi |
+-------------+----------------------+
Received a total of 3 rows
!ok
```

### Multiple Tags

In some cases, the terminal gets different kinds of the message from the executor. Please use the
highest order of fatality to identify the output.

Example:

```
# test set the deprecated key
SET execution.planner=blink;
[WARNING] The specified key 'execution.planner' is deprecated. Please use 'table.planner' instead.
[INFO] Session property has been set.
!warning
```

## Limitation

Currently, the SQL Script can't support to test some interactive feature of SQL Client, including:

- Statement completion.
- The `changelog` and `table` result mode which visualizes results in a refreshable paginated table representation.
- `INSERT INTO` statement. Because all DMLs are executed asynchronously, so we can't check the content written into the sink. This can be supported once FLINK-21669 is resolved.

