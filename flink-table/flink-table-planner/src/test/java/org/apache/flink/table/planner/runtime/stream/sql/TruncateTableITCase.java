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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The IT case for Truncate table statement in streaming mode. */
public class TruncateTableITCase extends StreamingTestBase {

    @Test
    void testTruncateTable() {
        // should throw exception since truncate table is not support in streaming mode
        tEnv().executeSql("CREATE TABLE t (a int) WITH ('connector' = 'test-update-delete')");
        assertThatThrownBy(() -> tEnv().executeSql("TRUNCATE TABLE t"))
                .isInstanceOf(TableException.class)
                .hasMessage("TRUNCATE TABLE statement is not supported in streaming mode.");
    }
}
