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

package org.apache.flink.table.planner.plan.stream.sql.join;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecJoin;
import org.apache.flink.table.planner.utils.JsonTestUtils;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the upsert keys the planner passes to {@link StreamExecJoin}. */
class JoinUpsertKeyOrderTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @BeforeEach
    void setup() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE a (a1 BIGINT, a2 STRING, PRIMARY KEY (a1) NOT ENFORCED)"
                                + " WITH ('connector' = 'values', 'changelog-mode' = 'I,UB,UA,D')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b (b1 BIGINT, b2 STRING, PRIMARY KEY (b1) NOT ENFORCED)"
                                + " WITH ('connector' = 'values', 'changelog-mode' = 'I,UB,UA,D')");
        util.tableEnv()
                .executeSql("CREATE TABLE c (c1 STRING, c2 STRING) WITH ('connector' = 'values')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink (a1 BIGINT, b1 BIGINT, c2 STRING)"
                                + " WITH ('connector' = 'blackhole')");
    }

    @Test
    void testUpsertKeysOfEqualLengthHaveStableOrder() throws Exception {
        // a JOIN b is 1:1, so the left input of the second join has the upsert keys {a1}, {b1}
        // and {a1, b1}, none of them in its join key. The first of the shortest listed keys
        // becomes the key of the join state, so the order must be the same on every compilation.
        for (int i = 0; i < 10; i++) {
            assertThat(
                            leftUpsertKeysOfJoins(
                                    "INSERT INTO sink SELECT a1, b1, c2 FROM "
                                            + "(SELECT * FROM a JOIN b ON a1 = b1) "
                                            + "JOIN c ON a2 = c1"))
                    .containsExactly("[[0]]", "[[0],[2],[0,2]]");
        }
    }

    private List<String> leftUpsertKeysOfJoins(String insert) throws Exception {
        final JsonNode plan =
                JsonTestUtils.readFromString(util.tableEnv().compilePlanSql(insert).asJsonString());
        final List<String> keys = new ArrayList<>();
        for (JsonNode node : plan.get("nodes")) {
            if (node.get("type").asText().startsWith("stream-exec-join_")) {
                keys.add(node.get(StreamExecJoin.FIELD_NAME_LEFT_UPSERT_KEYS).toString());
            }
        }
        return keys;
    }
}
