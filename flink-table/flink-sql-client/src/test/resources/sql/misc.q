# misc.q - Miscellaneous statements
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# === test help ===
help;
The following commands are available:

HELP               		Prints the available commands.
QUIT/EXIT          		Quits the SQL CLI client.
CLEAR              		Clears the current terminal.
SET                		Sets a session configuration property. Syntax: "SET '<key>'='<value>';". Use "SET;" for listing all properties.
RESET              		Resets a session configuration property. Syntax: "RESET '<key>';". Use "RESET;" for reset all session properties.
INSERT INTO        		Inserts the results of a SQL SELECT query into a declared table sink.
INSERT OVERWRITE   		Inserts the results of a SQL SELECT query into a declared table sink and overwrite existing data.
SELECT             		Executes a SQL SELECT query on the Flink cluster.
EXPLAIN            		Describes the execution plan of a query or table with the given name.
BEGIN STATEMENT SET		Begins a statement set. Syntax: "BEGIN STATEMENT SET;"
END                		Ends a statement set. Syntax: "END;"
ADD JAR            		Adds the specified jar file to the submitted jobs' classloader. Syntax: "ADD JAR '<path_to_filename>.jar'"
REMOVE JAR         		Removes the specified jar file from the submitted jobs' classloader. Syntax: "REMOVE JAR '<path_to_filename>.jar'"
SHOW JARS          		Shows the list of user-specified jar dependencies. This list is impacted by the --jar and --library startup options as well as the ADD/REMOVE JAR commands.

Hint: Make sure that a statement ends with ";" for finalizing (multi-line) statements.
You can also type any Flink SQL statement, please visit https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/ for more details.
!ok
