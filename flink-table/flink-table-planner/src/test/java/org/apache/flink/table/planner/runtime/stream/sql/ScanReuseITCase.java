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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** IT Case for push project into source with sub plan reuse. */
public class ScanReuseITCase extends StreamingTestBase {

    @Before
    public void before() throws Exception {
        super.before();
        String myTableDataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of(1, 1L, "1"), Row.of(2, 2L, "2"), Row.of(3, 3L, "3")));
        String table =
                "CREATE TABLE MyTable (\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true',\n"
                        + String.format(" 'data-id' = '%s',\n", myTableDataId)
                        + " 'nested-projection-supported' = 'true',\n"
                        + " 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING'\n"
                        + ")";
        tEnv().executeSql(table);
    }

    @Test
    public void testProjectWithExpr() {
        String sqlQuery =
                "SELECT T1.a, T1.b, T2.c FROM"
                        + " (SELECT a, b + 1 as b FROM MyTable) T1, MyTable T2 WHERE T1.a = T2.a";
        List<String> actual =
                CollectionUtil.iteratorToList(tEnv().executeSql(sqlQuery).collect()).stream()
                        .map(Object::toString)
                        .collect(Collectors.toList());
        actual.sort(String::compareTo);
        List<String> expected = Arrays.asList("+I[1, 2, 1]", "+I[2, 3, 2]", "+I[3, 4, 3]");
        assertEquals(expected, actual);
    }
}
