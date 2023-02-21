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

package org.apache.flink.table.planner.runtime.stream.sql.join;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.runtime.utils.JoinReorderITCaseBase;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.table.planner.runtime.utils.TestingRetractSink;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for JoinReorder in stream mode. */
public class JoinReorderITCase extends JoinReorderITCaseBase {

    private StreamExecutionEnvironment env;

    @AfterEach
    public void after() {
        super.after();
        StreamTestSink.clear();
    }

    @Override
    protected TableEnvironment getTableEnvironment() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        return StreamTableEnvironment.create(env, settings);
    }

    @Override
    protected void assertEquals(String query, List<String> expectedList) {
        StreamTableEnvironment streamTableEnvironment = (StreamTableEnvironment) tEnv;
        Table table = streamTableEnvironment.sqlQuery(query);
        TestingRetractSink sink = new TestingRetractSink();
        streamTableEnvironment
                .toRetractStream(table, Row.class)
                .map(JavaScalaConversionUtil::toScala, TypeInformation.of(Tuple2.class))
                .addSink((SinkFunction) sink);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<String> results = JavaScalaConversionUtil.toJava(sink.getRetractResults());
        results = new ArrayList<>(results);
        results.sort(String::compareTo);
        expectedList.sort(String::compareTo);

        assertThat(results).isEqualTo(expectedList);
    }
}
