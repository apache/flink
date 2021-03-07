/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.xa;

import org.apache.flink.api.common.restartstrategy.RestartStrategies.NoRestartStrategyConfiguration;
import org.apache.flink.connector.jdbc.DbMetadata;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcITCase;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.*;

/** A simple end-to-end test for {@link JdbcXaSinkFunction}. */
public class JdbcDynamicExactlyOnceSinkE2eTest extends JdbcXaSinkTestBase {

    @Override
    @Before
    public void initHelpers() throws Exception {
        xaDataSource = getDbMetadata().buildXaDataSource();
        xaHelper =
                new JdbcXaFacadeTestHelper(
                        getDbMetadata().buildXaDataSource(),
                        getDbMetadata().getUrl(),
                        Arrays.asList(INPUT_TABLE, INPUT_TABLE_2));
        sinkHelper = buildSinkHelper(createStateHandler());
    }

    @Test
    public void testDynamicTableInsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(new NoRestartStrategyConfiguration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(50, CheckpointingMode.EXACTLY_ONCE);
        env.addSource(new CheckpointAwaitingSource<>(Arrays.asList(TEST_DATA)))
                .returns(TestEntry.class)
                .addSink(
                        JdbcSink.exactlyOnceSinkWithDynamicOutput(
                                (elem) -> {
                                    if (elem.id % 2 == 0) {
                                        return String.format(INSERT_TEMPLATE, INPUT_TABLE);
                                    } else {
                                        return String.format(INSERT_TEMPLATE, INPUT_TABLE_2);
                                    }
                                },
                                JdbcITCase.TEST_ENTRY_JDBC_STATEMENT_BUILDER,
                                (elem) -> {
                                    if (elem.id % 2 == 0) {
                                        return "1";
                                    } else {
                                        return "2";
                                    }
                                },
                                JdbcExecutionOptions.builder().build(),
                                JdbcExactlyOnceOptions.defaults(),
                                DERBY_EBOOKSHOP_DB::buildXaDataSource));
        env.execute();
        xaHelper.assertDbContentsEquals(
                Arrays.asList(1002, 1004, 1006, 1008, 1010, 1001, 1003, 1005, 1007, 1009));
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return DERBY_EBOOKSHOP_DB;
    }
}
