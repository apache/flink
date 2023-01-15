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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The IT case for UPDATE statement in streaming mode. */
public class UpdateTableITCase extends StreamingTestBase {

    @Test
    public void testUpdate() throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE t ("
                                + " a int PRIMARY KEY NOT ENFORCED,"
                                + " b string,"
                                + " c double) WITH"
                                + " ('connector' = 'test-update-delete')");
        assertThatThrownBy(
                        () -> tEnv().executeSql("UPDATE t SET b = 'uaa', c = c * c WHERE a >= 1"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("UPDATE statement is not supported for streaming mode now.");
    }
}
