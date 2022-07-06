/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.calcite;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkPlannerImpl} that require a planner. */
public class FlinkPlannerImplTest {
    @Test
    public void testCreateSqlValidator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<String> testStream =
                env.fromCollection(Arrays.asList("beer", "disappear", "beer"));

        // convert DataStream to Table
        tEnv.createTemporaryView("test", testStream, $("user"));

        // union the two tables
        Table resultByName =
                tEnv.sqlQuery("SELECT user, count(*) as amount FROM test group by user");
        Table resultByPos = tEnv.sqlQuery("SELECT user, count(*) as amount FROM test group by 1");

        List<Tuple2<Boolean, Row>> resultsByName =
                CollectionUtil.iteratorToList(
                        tEnv.toRetractStream(resultByName, Row.class).executeAndCollect());
        List<Tuple2<Boolean, Row>> resultsByPos =
                CollectionUtil.iteratorToList(
                        tEnv.toRetractStream(resultByPos, Row.class).executeAndCollect());
        assertThat(resultsByName).isEqualTo(resultsByPos);

        assertThat(resultsByPos).hasSize(4);
        assertThat(resultsByPos.get(0).f0).isEqualTo(true);
        assertThat(resultsByPos.get(2).f0).isEqualTo(false);
        assertThat(resultsByPos.get(1).f1.getField(1)).isEqualTo(1L);
        assertThat(resultsByPos.get(3).f1.getField(1)).isEqualTo(2L);
    }
}
