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

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.planner.runtime.utils.TestData;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for `ANALYZE TABLE`. */
public class AnalyzeTableITCase extends StreamingTestBase {

    private TableEnvironment tEnv;

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        tEnv = tEnv();
        String dataId1 = TestValuesTableFactory.registerData(TestData.smallData3());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE MyTable (\n"
                                + "  `a` INT,\n"
                                + "  `b` BIGINT,\n"
                                + "  `c` VARCHAR\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'data-id' = '%s',\n"
                                + "  'bounded' = 'true'\n"
                                + ")",
                        dataId1));
    }

    @Test
    public void testAnalyzeTable() {
        assertThatThrownBy(() -> tEnv.executeSql("analyze table MyTable compute statistics"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("ANALYZE TABLE is not supported for streaming mode now");
    }
}
