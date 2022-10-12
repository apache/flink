/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.cli.parser;

/** Enumerates the possible types of input statements. */
public enum StatementType {
    /** Command to quit the client. */
    QUIT,
    /** Command to clear the terminal's screen. */
    CLEAR,
    /** Command to print help message. */
    HELP,
    /** Command to set session configuration property. */
    SET,
    /** Command to reset session configuration property or reset all properties. */
    RESET,
    /** 'EXPLAIN' SQL statement. */
    EXPLAIN,
    /** 'SHOW CREATE TABLE/VIEW' SQL statement. */
    SHOW_CREATE,
    /** 'BEGIN STATEMENT SET;' SQL statement. */
    BEGIN_STATEMENT_SET,
    /** 'END;' SQL statement. */
    END,
    /** 'REMOVE JAR' SQL statement. */
    REMOVE_JAR,
    /** 'SELECT' SQL statement. */
    SELECT,
    /** Type not covered by any other type value. */
    OTHER
}
