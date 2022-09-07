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

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/** IT test for filter condition. */
public class FilterITCase extends BatchTestBase {

    private TableEnvironment tEnv;

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        tEnv = tEnv();
        String dataId1 =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(1, true, "true"),
                                Row.of(2, false, "true"),
                                Row.of(3, true, "false")));
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE MyTable (\n"
                                + "  `a` INT,\n"
                                + "  `b` BOOLEAN,\n"
                                + "  `c` VARCHAR\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        dataId1));
    }

    @Test
    public void testFilterWithStringEqualsTrueInAndCondition() {
        // TODO Flink now doesn't support implicate type conversion. So filter condition 'c = true'
        // can not convert to 'c = "true"', and the test will get an empty result instead of
        // returning results: 'Row(1, true, "true")'.
        checkResult(
                "SELECT * FROM MyTable WHERE b = true and c = true",
                JavaScalaConversionUtil.toScala(Collections.emptyList()),
                false);
    }

    @Test
    public void testFilterWithStringEqualsTrue() {
        // TODO Flink now doesn't support implicate type conversion. So filter condition 'c = true'
        // can not convert to 'c = "true"', and the test will get an empty result instead of
        // returning results: 'Row(1, true, "true"), Row(2, false, "true")'.
        checkResult(
                "SELECT * FROM MyTable WHERE c = true",
                JavaScalaConversionUtil.toScala(Collections.emptyList()),
                false);
    }

    @Test
    public void testFilterWithBooleanIsAlwaysTrue() {
        checkResult(
                "SELECT * FROM MyTable WHERE b = true",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(Row.of(1, true, "true"), Row.of(3, true, "false"))),
                false);
    }
}
