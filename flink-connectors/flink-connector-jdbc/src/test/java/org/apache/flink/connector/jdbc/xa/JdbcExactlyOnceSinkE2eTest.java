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
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.junit.Test;

import java.io.Serializable;
import java.util.stream.IntStream;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;

/** A simple end-to-end test for {@link JdbcXaSinkFunction}. */
public class JdbcExactlyOnceSinkE2eTest extends JdbcXaSinkTestBase {

    @Test
    public void testInsert() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(new NoRestartStrategyConfiguration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(50, CheckpointingMode.EXACTLY_ONCE);
        env.addSource(new CheckpointAwaitingSource<>(TEST_DATA))
                .returns(TestEntry.class)
                .addSink(
                        JdbcSink.exactlyOnceSink(
                                String.format(INSERT_TEMPLATE, INPUT_TABLE),
                                JdbcITCase.TEST_ENTRY_JDBC_STATEMENT_BUILDER,
                                JdbcExecutionOptions.builder().build(),
                                DERBY_EBOOKSHOP_DB.toConnectionOptions(),
                                JdbcExactlyOnceOptions.defaults(),
                                DERBY_EBOOKSHOP_DB::buildXaDataSource));
        env.execute();
        xaHelper.assertDbContentsEquals(IntStream.range(0, TEST_DATA.length));
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return DERBY_EBOOKSHOP_DB;
    }

    /** {@link SourceFunction} emits all the data and waits for the checkpoint. */
    private static class CheckpointAwaitingSource<T extends Serializable>
            implements SourceFunction<T>, CheckpointListener, CheckpointedFunction {
        private volatile boolean allDataEmitted = false;
        private volatile boolean dataCheckpointed = false;
        private volatile boolean running = true;
        private volatile long checkpointAfterData = -1L;
        private final T[] data;

        private CheckpointAwaitingSource(T... data) {
            this.data = data;
        }

        @Override
        public void run(SourceContext<T> ctx) {
            for (T datum : data) {
                ctx.collect(datum);
            }
            allDataEmitted = true;
            while (!dataCheckpointed && running) {
                Thread.yield();
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (checkpointId == this.checkpointAfterData) {
                dataCheckpointed = true;
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            if (allDataEmitted) {
                checkpointAfterData = context.getCheckpointId();
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}
    }
}
