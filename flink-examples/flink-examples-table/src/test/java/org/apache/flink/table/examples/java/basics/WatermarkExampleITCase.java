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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for Java Watermark Query in Batch and Streaming mode. */
class WatermarkExampleITCase {
    private static final String createTableA =
            "CREATE TABLE a ("
                    + "testTime BIGINT NOT NULL, "
                    + "eventTime AS TO_TIMESTAMP_LTZ(testTime, 3), "
                    + "WATERMARK FOR eventTime AS eventTime - INTERVAL '5' SECOND"
                    + ") WITH ("
                    + "'connector' = 'datagen', "
                    + "'number-of-rows' = '10'"
                    + ")";
    private static final String createTableB =
            "CREATE TABLE b ("
                    + "testTime BIGINT NOT NULL, "
                    + "eventTime TIMESTAMP_LTZ(3), "
                    + "WATERMARK FOR eventTime AS eventTime - INTERVAL '5' SECOND"
                    + ") WITH ("
                    + "'connector' = 'filesystem', "
                    + "'format' = 'csv', "
                    + "'path' = '/tmp/'"
                    + ")";
    private static final String insertIntoB = "INSERT INTO b SELECT * FROM a";
    private static final String selectFromB = "SELECT * FROM b";
    private static final String testTimeStr = "testTime";
    private static final String eventTimeStr = "eventTime";

    @Test
    void testWatermarkInBatchMode() {
        Configuration configuration = new Configuration();
        configuration.setString(
                ExecutionOptions.RUNTIME_MODE.key(), RuntimeExecutionMode.BATCH.toString());
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(createTableA);
        tEnv.executeSql(createTableB);
        tEnv.executeSql(insertIntoB);

        TableResult res = tEnv.executeSql(selectFromB);
        assertThat(res.getResolvedSchema().toString().contains(testTimeStr)).isTrue();
        assertThat(res.getResolvedSchema().toString().contains(eventTimeStr)).isTrue();
    }

    @Test
    void testWatermarkInStreamingMode() {
        Configuration configuration = new Configuration();
        configuration.setString(
                ExecutionOptions.RUNTIME_MODE.key(), RuntimeExecutionMode.STREAMING.toString());
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(createTableA);
        tEnv.executeSql(createTableB);
        tEnv.executeSql(insertIntoB);

        TableResult res = tEnv.executeSql(selectFromB);
        assertThat(res.getResolvedSchema().toString().contains(testTimeStr)).isTrue();
        assertThat(res.getResolvedSchema().toString().contains(eventTimeStr)).isTrue();
    }
}
