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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.DbMetadata;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcITCase;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.postgresql.xa.PGXADataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.XADataSource;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.connector.jdbc.xa.JdbcXaFacadeTestHelper.getInsertedIds;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertTrue;

/** A simple end-to-end test for {@link JdbcXaSinkFunction}. */
// todo: unignore in FLINK-22462 or earlier
@Ignore
public class JdbcExactlyOnceSinkE2eTest extends JdbcTestBase {

    private static final class PgXaDb extends PostgreSQLContainer<PgXaDb> {
        public PgXaDb(String dockerImageName) {
            super(dockerImageName);
            // set max_prepared_transactions to non-zero
            this.setCommand("postgres", "-c", "max_prepared_transactions=50", "-c", "fsync=off");
        }
    }

    @Rule public PgXaDb db = new PgXaDb("postgres:9.6.12");

    @Override
    public void after() throws Exception {
        // no need for cleanup - done by test container tear down
    }

    @Test
    public void testInsert() throws Exception {
        int parallelism = 4;
        int elementsPerSource = 500;
        int numElementsPerCheckpoint = 7;
        int minElementsPerFailure = numElementsPerCheckpoint / 3;
        int maxElementsPerFailure = numElementsPerCheckpoint * 3;

        Configuration configuration = new Configuration();
        configuration.set(
                EXECUTION_FAILOVER_STRATEGY,
                "full" /* allow checkpointing even after some sources have finished */);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, Time.milliseconds(100)));
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(50, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(parallelism);
        env.disableOperatorChaining();
        String password = db.getPassword();
        String username = db.getUsername();
        String jdbcUrl = db.getJdbcUrl();

        env.addSource(new TestEntrySource(elementsPerSource, numElementsPerCheckpoint))
                .setParallelism(parallelism)
                .map(new FailingMapper(minElementsPerFailure, maxElementsPerFailure))
                .addSink(
                        JdbcSink.exactlyOnceSink(
                                String.format(INSERT_TEMPLATE, INPUT_TABLE),
                                JdbcITCase.TEST_ENTRY_JDBC_STATEMENT_BUILDER,
                                JdbcExecutionOptions.builder().build(),
                                JdbcExactlyOnceOptions.defaults(),
                                () -> getXaDataSource(jdbcUrl, username, password)));
        env.execute();

        List<Integer> insertedIds = getInsertedIds(jdbcUrl, username, password, INPUT_TABLE);
        List<Integer> expectedIds =
                IntStream.range(0, elementsPerSource * parallelism)
                        .boxed()
                        .collect(Collectors.toList());
        assertTrue(
                insertedIds.toString(),
                insertedIds.size() == expectedIds.size() && expectedIds.containsAll(insertedIds));
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return new DbMetadata() {
            @Override
            public String getInitUrl() {
                return db.getJdbcUrl();
            }

            @Override
            public String getUrl() {
                return db.getJdbcUrl();
            }

            @Override
            public XADataSource buildXaDataSource() {
                return getXaDataSource(db.getJdbcUrl(), db.getUsername(), db.getPassword());
            }

            @Override
            public String getDriverClass() {
                return db.getDriverClassName();
            }

            @Override
            public String getUser() {
                return db.getUsername();
            }

            @Override
            public String getPassword() {
                return db.getPassword();
            }
        };
    }

    /** {@link SourceFunction} emits {@link TestEntry test entries} and waits for the checkpoint. */
    private static class TestEntrySource extends RichParallelSourceFunction<TestEntry>
            implements CheckpointListener, CheckpointedFunction {
        private final int numElements;
        private final int numElementsPerCheckpoint;

        private transient ListState<SourceRange> ranges;

        private volatile boolean allDataEmitted = false;
        private volatile boolean snapshotTaken = false;
        private volatile long lastCheckpointId = -1L;
        private volatile boolean lastSnapshotConfirmed = false;
        private volatile boolean running = true;
        private static volatile CountDownLatch runningSources;

        private TestEntrySource(int numElements, int numElementsPerCheckpoint) {
            this.numElements = numElements;
            this.numElementsPerCheckpoint = numElementsPerCheckpoint;
        }

        @Override
        public void run(SourceContext<TestEntry> ctx) throws Exception {
            for (SourceRange range : ranges.get()) {
                for (int i = range.from; i < range.to; ) {
                    synchronized (ctx.getCheckpointLock()) {
                        snapshotTaken = false;
                        for (int j = 0; j < numElementsPerCheckpoint && i < range.to; j++, i++) {
                            emit(ctx, i);
                            range.advance();
                        }
                    }
                    sleep(() -> !snapshotTaken);
                }
            }
            allDataEmitted = true;
            sleep(() -> !lastSnapshotConfirmed);
            runningSources.countDown();
            runningSources.await(); // participate in checkpointing
        }

        private void emit(SourceContext<TestEntry> ctx, int i) {
            ctx.collect(new TestEntry(i, Integer.toString(i), Integer.toString(i), (double) i, i));
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (lastCheckpointId > -1L && checkpointId >= this.lastCheckpointId) {
                lastSnapshotConfirmed = true;
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                runningSources =
                        new CountDownLatch(getRuntimeContext().getNumberOfParallelSubtasks());
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ranges =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("SourceState", SourceRange.class));
            if (!context.isRestored()) {
                ranges.update(
                        singletonList(
                                SourceRange.forSubtask(
                                        getRuntimeContext().getIndexOfThisSubtask(), numElements)));
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            snapshotTaken = true;
            if (allDataEmitted) {
                lastCheckpointId = context.getCheckpointId();
            }
        }

        private void sleep(Supplier<Boolean> condition) {
            while (condition.get() && running && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ExceptionUtils.rethrow(e);
                }
            }
        }

        private static final class SourceRange {
            private int from;
            private final int to;

            private SourceRange(int from, int to) {
                this.from = from;
                this.to = to;
            }

            public static SourceRange forSubtask(int subtaskIndex, int elementCount) {
                return new SourceRange(
                        subtaskIndex * elementCount, (subtaskIndex + 1) * elementCount);
            }

            public void advance() {
                checkState(from < to);
                from++;
            }
        }
    }

    private static XADataSource getXaDataSource(String jdbcUrl, String username, String password) {
        PGXADataSource xaDataSource = new PGXADataSource();
        xaDataSource.setUrl(jdbcUrl);
        xaDataSource.setUser(username);
        xaDataSource.setPassword(password);
        return xaDataSource;
    }

    private static class FailingMapper extends RichMapFunction<TestEntry, TestEntry> {
        private final int minElementsPerFailure;
        private final int maxElementsPerFailure;
        private transient int remaining;

        public FailingMapper(int minElementsPerFailure, int maxElementsPerFailure) {
            this.minElementsPerFailure = minElementsPerFailure;
            this.maxElementsPerFailure = maxElementsPerFailure;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            remaining = minElementsPerFailure + new Random().nextInt(maxElementsPerFailure);
        }

        @Override
        public TestEntry map(TestEntry value) throws Exception {
            if (--remaining <= 0) {
                throw new TestException();
            }
            return value;
        }
    }

    private static final class TestException extends Exception {
        public TestException() {
            super("expected", null, true, false);
        }
    }
}
