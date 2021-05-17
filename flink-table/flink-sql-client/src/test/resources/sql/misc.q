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

CLEAR		Clears the current terminal.
CREATE TABLE		Create table under current catalog and database.
DROP TABLE		Drop table with optional catalog and database. Syntax: 'DROP TABLE [IF EXISTS] <name>;'
CREATE VIEW		Creates a virtual table from a SQL query. Syntax: 'CREATE VIEW <name> AS <query>;'
DESCRIBE		Describes the schema of a table with the given name.
DROP VIEW		Deletes a previously created virtual table. Syntax: 'DROP VIEW <name>;'
EXPLAIN		Describes the execution plan of a query or table with the given name.
HELP		Prints the available commands.
INSERT INTO		Inserts the results of a SQL SELECT query into a declared table sink.
INSERT OVERWRITE		Inserts the results of a SQL SELECT query into a declared table sink and overwrite existing data.
QUIT		Quits the SQL CLI client.
RESET		Resets a session configuration property. Syntax: 'RESET <key>;'. Use 'RESET;' for reset all session properties.
SELECT		Executes a SQL SELECT query on the Flink cluster.
SET		Sets a session configuration property. Syntax: 'SET <key>=<value>;'. Use 'SET;' for listing all properties.
SHOW FUNCTIONS		Shows all user-defined and built-in functions or only user-defined functions. Syntax: 'SHOW [USER] FUNCTIONS;'
SHOW TABLES		Shows all registered tables.
SOURCE		Reads a SQL SELECT query from a file and executes it on the Flink cluster.
USE CATALOG		Sets the current catalog. The current database is set to the catalog's default one. Experimental! Syntax: 'USE CATALOG <name>;'
USE		Sets the current default database. Experimental! Syntax: 'USE <name>;'
LOAD MODULE		Load a module. Syntax: 'LOAD MODULE <name> [WITH ('<key1>' = '<value1>' [, '<key2>' = '<value2>', ...])];'
UNLOAD MODULE		Unload a module. Syntax: 'UNLOAD MODULE <name>;'
USE MODULES		Enable loaded modules. Syntax: 'USE MODULES <name1> [, <name2>, ...];'
BEGIN STATEMENT SET		Begins a statement set. Syntax: 'BEGIN STATEMENT SET;'
END		Ends a statement set. Syntax: 'END;'

Hint: Make sure that a statement ends with ';' for finalizing (multi-line) statements.
!ok
