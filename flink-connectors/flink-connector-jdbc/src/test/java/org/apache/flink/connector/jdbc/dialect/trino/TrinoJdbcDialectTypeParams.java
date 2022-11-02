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

package org.apache.flink.connector.jdbc.dialect.trino;

import org.apache.flink.connector.jdbc.dialect.JdbcDialectTypeTest;

import java.util.Arrays;
import java.util.List;

/** The Trino params for {@link JdbcDialectTypeTest.JdbcDialectTypeParam}. */
public class TrinoJdbcDialectTypeParams extends JdbcDialectTypeTest.JdbcDialectTypeParam {

    @Override
    protected List<JdbcDialectTypeTest.TestItem> testData() {
        return Arrays.asList(
                createTestItem("trino", "BOOLEAN"),
                createTestItem("trino", "TINYINT"),
                createTestItem("trino", "SMALLINT"),
                createTestItem("trino", "INTEGER"),
                createTestItem("trino", "BIGINT"),
                createTestItem("trino", "DOUBLE"),
                createTestItem("trino", "DECIMAL(10, 4)"),
                createTestItem("trino", "DECIMAL(38, 18)"),
                createTestItem("trino", "VARCHAR"),
                createTestItem("trino", "CHAR"),
                createTestItem("trino", "VARBINARY"),
                createTestItem("trino", "DATE"),
                createTestItem("trino", "TIME"),
                createTestItem("trino", "TIMESTAMP(3)"),
                createTestItem("trino", "TIMESTAMP WITHOUT TIME ZONE"),
                createTestItem("trino", "TIMESTAMP(9) WITHOUT TIME ZONE"),

                // Not valid data
                createTestItem("trino", "FLOAT", "The Trino dialect doesn't support type: FLOAT."));
    }
}
