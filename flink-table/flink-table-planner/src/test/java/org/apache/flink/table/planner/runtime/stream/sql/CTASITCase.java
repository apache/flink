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

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.SetSemanticTableFunction;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.planner.runtime.utils.TestData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for CREATE TABLE AS SELECT statement over a {@link ProcessTableFunction}. */
class CTASITCase extends StreamingTestBase {

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        final String dataId = TestValuesTableFactory.registerData(TestData.smallData3());
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE source(a int, b bigint, c string)"
                                        + " WITH ('connector' = 'values', 'bounded' = 'true', 'data-id' = '%s')",
                                dataId));
        tEnv().createTemporarySystemFunction("f", SetSemanticTableFunction.class);
    }

    @Test
    void testCreateTableAsSelectOverSetSemanticPtfWithReorderedColumns() throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE sink (`out`, `a`) WITH ('connector' = 'values') AS "
                                + "SELECT * FROM f(r => TABLE source PARTITION BY a, i => 1)")
                .await();

        final ResolvedSchema schema = tEnv().from("sink").getResolvedSchema();
        assertThat(schema.getColumnNames()).containsExactly("out", "a");

        assertThat(TestValuesTableFactory.getResultsAsStrings("sink"))
                .containsExactlyInAnyOrder(
                        "+I[{+I[1, 1, Hi], 1}, 1]",
                        "+I[{+I[2, 2, Hello], 1}, 2]",
                        "+I[{+I[3, 2, Hello world], 1}, 3]");
    }
}
