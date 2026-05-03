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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SqlShowToOperationConverterTest extends SqlNodeToOperationConversionTestBase {

    @BeforeEach
    void before() throws TableAlreadyExistException, DatabaseNotExistException {
        // Do nothing
        // No need to create schema, tables and etc. since the test executes for unset catalog and
        // database
    }

    @AfterEach
    void after() throws TableNotExistException {
        // Do nothing
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "SHOW TABLES",
                "SHOW VIEWS",
                "SHOW FUNCTIONS",
                "SHOW PROCEDURES",
                "SHOW MATERIALIZED TABLES"
            })
    void testParseShowFunctionForUnsetCatalog(String sql) {
        catalogManager.setCurrentCatalog(null);
        // No exception should be thrown during parsing.
        // Validation exception should be thrown while execution.
        parse(sql);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "SHOW TABLES",
                "SHOW VIEWS",
                "SHOW FUNCTIONS",
                "SHOW PROCEDURES",
                "SHOW MATERIALIZED TABLES"
            })
    void testParseShowFunctionForUnsetDatabase(String sql) {
        catalogManager.setCurrentDatabase(null);
        // No exception should be thrown during parsing.
        // Validation exception should be thrown while execution.
        parse(sql);
    }
}
