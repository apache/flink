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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.types.AbstractDataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT Case for [CREATE OR] REPLACE TABLE AS SELECT statement. */
class RTASITCase extends BatchTestBase {

    @BeforeEach
    @Override
    public void before() {
        String dataId1 = TestValuesTableFactory.registerData(TestData.smallData3());
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE source(a int, b bigint, c string)"
                                        + " WITH ('connector' = 'values', 'bounded' = 'true', 'data-id' = '%s')",
                                dataId1));
        tEnv().executeSql(
                        "CREATE TABLE target(a int, b bigint, c string)"
                                + " WITH ('connector' = 'values')");
    }

    @Test
    void testReplaceTableAS() throws Exception {
        tEnv().executeSql(
                        "REPLACE TABLE target WITH ('connector' = 'values',"
                                + " 'bounded' = 'true')"
                                + " AS SELECT * FROM source")
                .await();

        // verify written rows
        assertThat(TestValuesTableFactory.getResultsAsStrings("target").toString())
                .isEqualTo("[+I[1, 1, Hi], +I[2, 2, Hello], +I[3, 2, Hello world]]");

        // verify the table after replacing
        CatalogTable expectCatalogTable =
                getExpectCatalogTable(
                        new String[] {"a", "b", "c"},
                        new AbstractDataType[] {
                            DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()
                        });
        verifyCatalogTable(expectCatalogTable, getCatalogTable("target"));
    }

    @Test
    void testReplaceTableASWithTableNotExist() {
        assertThatThrownBy(() -> tEnv().executeSql("REPLACE TABLE t AS SELECT * FROM source"))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "The table `default_catalog`.`default_database`.`t` to be replaced doesn't exist."
                                + " You can try to use CREATE TABLE AS statement or CREATE OR REPLACE TABLE AS statement.");
    }

    @Test
    void testCreateOrReplaceTableAS() throws Exception {
        tEnv().executeSql(
                        "CREATE OR REPLACE TABLE target WITH ('connector' = 'values',"
                                + " 'bounded' = 'true')"
                                + " AS SELECT a, c FROM source")
                .await();

        // verify written rows
        assertThat(TestValuesTableFactory.getResultsAsStrings("target").toString())
                .isEqualTo("[+I[1, Hi], +I[2, Hello], +I[3, Hello world]]");

        // verify the table after replacing
        CatalogTable expectCatalogTable =
                getExpectCatalogTable(
                        new String[] {"a", "c"},
                        new AbstractDataType[] {DataTypes.INT(), DataTypes.STRING()});
        verifyCatalogTable(expectCatalogTable, getCatalogTable("target"));
    }

    @Test
    void testCreateOrReplaceTableASWithTableNotExist() throws Exception {
        tEnv().executeSql(
                        "CREATE OR REPLACE TABLE not_exist_target WITH ('connector' = 'values',"
                                + " 'bounded' = 'true')"
                                + " AS SELECT a, c FROM source")
                .await();

        // verify written rows
        assertThat(TestValuesTableFactory.getResultsAsStrings("not_exist_target").toString())
                .isEqualTo("[+I[1, Hi], +I[2, Hello], +I[3, Hello world]]");

        // verify the table after replacing
        CatalogTable expectCatalogTable =
                getExpectCatalogTable(
                        new String[] {"a", "c"},
                        new AbstractDataType[] {DataTypes.INT(), DataTypes.STRING()});
        verifyCatalogTable(expectCatalogTable, getCatalogTable("not_exist_target"));
    }

    private CatalogTable getExpectCatalogTable(
            String[] cols, AbstractDataType<?>[] fieldDataTypes) {
        return CatalogTable.of(
                Schema.newBuilder().fromFields(cols, fieldDataTypes).build(),
                null,
                Collections.emptyList(),
                getDefaultTargetTableOptions());
    }

    private Map<String, String> getDefaultTargetTableOptions() {
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("connector", "values");
        expectedOptions.put("bounded", "true");
        return expectedOptions;
    }

    private CatalogTable getCatalogTable(String tableName) throws TableNotExistException {
        return (CatalogTable)
                tEnv().getCatalog("default_catalog")
                        .get()
                        .getTable(ObjectPath.fromString("default_database." + tableName));
    }

    private void verifyCatalogTable(
            CatalogTable expectedCatalogTable, CatalogTable actualCatalogTable) {
        assertThat(actualCatalogTable.getUnresolvedSchema())
                .isEqualTo(expectedCatalogTable.getUnresolvedSchema());
        assertThat(actualCatalogTable.getComment()).isEqualTo(expectedCatalogTable.getComment());
        assertThat(actualCatalogTable.getPartitionKeys())
                .isEqualTo(expectedCatalogTable.getPartitionKeys());
        assertThat(actualCatalogTable.getOptions()).isEqualTo(expectedCatalogTable.getOptions());
    }
}
