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
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.SerializableSupplier;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.xa.PGXADataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.XADataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_CANCELLATION_TIMEOUT;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.connector.jdbc.xa.JdbcXaFacadeTestHelper.getInsertedIds;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertTrue;

/** A simple end-to-end test for {@link JdbcXaSinkFunction}. */
@RunWith(Parameterized.class)
public class JdbcExactlyOnceSinkE2eTest extends JdbcTestBase {
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    private static final Logger LOG = LoggerFactory.getLogger(JdbcExactlyOnceSinkE2eTest.class);

    private static final long CHECKPOINT_TIMEOUT_MS = 20_000L;
    private static final long TASK_CANCELLATION_TIMEOUT_MS = 20_000L;

    private interface JdbcExactlyOnceSinkTestEnv {
        void start();

        void stop();

        JdbcDatabaseContainer<?> getContainer();

        SerializableSupplier<XADataSource> getDataSourceSupplier();

        int getParallelism();
    }

    @Parameterized.Parameter public JdbcExactlyOnceSinkTestEnv dbEnv;

    private MiniClusterWithClientResource cluster;

    // track active sources for:
    // 1. if any cancels, cancel others ASAP
    // 2. wait for others (to participate in checkpointing)
    // not using SharedObjects because we want to explicitly control which tag (attempt) to use
    private static final Map<Integer, CountDownLatch> activeSources = new ConcurrentHashMap<>();
    // track inactive mappers - to start emission only when they are ready (to prevent them from
    // starving for memory)
    // not using SharedObjects because we want to explicitly control which tag (attempt) to use
    private static final Map<Integer, CountDownLatch> inactiveMappers = new ConcurrentHashMap<>();

    @Parameterized.Parameters(name = "{0}")
    public static Collection<JdbcExactlyOnceSinkTestEnv> parameters() {
        return Arrays.asList(
                // PGSQL: check for issues with suspending connections (requires pooling) and
                // honoring limits (properly closing connections).
                new PgSqlJdbcExactlyOnceSinkTestEnv(4),
                //            ,
                // MYSQL: check for issues with errors on closing connections.
                new MySqlJdbcExactlyOnceSinkTestEnv(4)
                // MSSQL - not testing: XA transactions need to be enabled via GUI (plus EULA).
                // DB2 - not testing: requires auth configuration (plus EULA).
                // MARIADB - not testing: XA rollback doesn't recognize recovered transactions.
                // ORACLE - not testing: an image needs to be built.
                );
    }

    @Before
    public void before() throws Exception {
        Configuration configuration = new Configuration();
        // single failover region to allow checkpointing even after some sources have finished and
        // restart all tasks if at least one fails
        configuration.set(EXECUTION_FAILOVER_STRATEGY, "full");
        // cancel tasks eagerly to reduce the risk of running out of memory with many restarts
        configuration.set(TASK_CANCELLATION_TIMEOUT, TASK_CANCELLATION_TIMEOUT_MS);
        configuration.set(CHECKPOINTING_TIMEOUT, Duration.ofMillis(CHECKPOINT_TIMEOUT_MS));
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                // Get enough TMs to run the job. Parallelize using TMs (rather than
                                // slots) for better isolation - this test tends to exhaust memory
                                // by restarts and fast sources
                                .setNumberTaskManagers(dbEnv.getParallelism())
                                .build());
        cluster.before();
        dbEnv.start();
        super.before();
    }

    @After
    @Override
    public void after() {
        // no need for cleanup - done by test container tear down
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
        dbEnv.stop();
        activeSources.clear();
        inactiveMappers.clear();
    }

    @Test
    public void testInsert() throws Exception {
        long started = System.currentTimeMillis();
        LOG.info("Test insert for {}", dbEnv);
        int elementsPerSource = 50;
        int numElementsPerCheckpoint = 7;
        int minElementsPerFailure = numElementsPerCheckpoint / 3;
        int maxElementsPerFailure = numElementsPerCheckpoint * 3;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(dbEnv.getParallelism());
        env.setRestartStrategy(fixedDelayRestart(Integer.MAX_VALUE, Time.milliseconds(100)));
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(50, CheckpointingMode.EXACTLY_ONCE);
        // timeout checkpoints as some tasks may fail while triggering
        env.getCheckpointConfig().setCheckpointTimeout(1000);
        // NOTE: keep operator chaining enabled to prevent memory exhaustion by sources while maps
        // are still initializing
        env.addSource(new TestEntrySource(elementsPerSource, numElementsPerCheckpoint))
                .setParallelism(dbEnv.getParallelism())
                .map(new FailingMapper(minElementsPerFailure, maxElementsPerFailure))
                .addSink(
                        JdbcSink.exactlyOnceSink(
                                String.format(INSERT_TEMPLATE, INPUT_TABLE),
                                JdbcITCase.TEST_ENTRY_JDBC_STATEMENT_BUILDER,
                                JdbcExecutionOptions.builder().build(),
                                JdbcExactlyOnceOptions.builder()
                                        .withTransactionPerConnection(true)
                                        .build(),
                                this.dbEnv.getDataSourceSupplier()));

        env.execute();

        List<Integer> insertedIds =
                getInsertedIds(
                        dbEnv.getContainer().getJdbcUrl(),
                        dbEnv.getContainer().getUsername(),
                        dbEnv.getContainer().getPassword(),
                        INPUT_TABLE);
        List<Integer> expectedIds =
                IntStream.range(0, elementsPerSource * dbEnv.getParallelism())
                        .boxed()
                        .collect(Collectors.toList());
        assertTrue(
                insertedIds.toString(),
                insertedIds.size() == expectedIds.size() && expectedIds.containsAll(insertedIds));
        LOG.info(
                "Test insert for {} finished in {}ms", dbEnv, System.currentTimeMillis() - started);
    }

    @Override
    protected DbMetadata getDbMetadata() {
        return new DbMetadata() {
            @Override
            public String getInitUrl() {
                return dbEnv.getContainer().getJdbcUrl();
            }

            @Override
            public String getUrl() {
                return dbEnv.getContainer().getJdbcUrl();
            }

            @Override
            public XADataSource buildXaDataSource() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getDriverClass() {
                return dbEnv.getContainer().getDriverClassName();
            }

            @Override
            public String getUser() {
                return dbEnv.getContainer().getUsername();
            }

            @Override
            public String getPassword() {
                return dbEnv.getContainer().getPassword();
            }
        };
    }

    /** {@link SourceFunction} emits {@link TestEntry test entries} and waits for the checkpoint. */
    private static class TestEntrySource extends RichParallelSourceFunction<TestEntry>
            implements CheckpointListener, CheckpointedFunction {
        private final int numElements;
        private final int numElementsPerCheckpoint;

        private transient volatile ListState<SourceRange> ranges;

        private volatile long lastCheckpointId = -1L;
        private volatile boolean lastSnapshotConfirmed = false;
        private volatile boolean running = true;

        private TestEntrySource(int numElements, int numElementsPerCheckpoint) {
            this.numElements = numElements;
            this.numElementsPerCheckpoint = numElementsPerCheckpoint;
        }

        @Override
        public void run(SourceContext<TestEntry> ctx) throws Exception {
            try {
                waitForConsumers();
                for (SourceRange range : ranges.get()) {
                    emitRange(range, ctx);
                }
            } finally {
                activeSources.get(getRuntimeContext().getAttemptNumber()).countDown();
            }
            waitOtherSources(); // participate in checkpointing
        }

        private void waitForConsumers() throws InterruptedException {
            // even though the pipeline is (intended to be) chained, other parallel instances may
            // starve if this source uses all the available memory before they initialize
            sleep(() -> !inactiveMappers.containsKey(getRuntimeContext().getAttemptNumber()));
            inactiveMappers.get(getRuntimeContext().getAttemptNumber()).await();
        }

        private void emitRange(SourceRange range, SourceContext<TestEntry> ctx) {
            for (int i = range.from; i < range.to && running; ) {
                int count = Math.min(range.to - i, numElementsPerCheckpoint);
                emit(i, count, range, ctx);
                i += count;
            }
        }

        private void emit(
                int start, int count, SourceRange toAdvance, SourceContext<TestEntry> ctx) {
            synchronized (ctx.getCheckpointLock()) {
                lastCheckpointId = -1L;
                lastSnapshotConfirmed = false;
                for (int j = start; j < start + count && running; j++) {
                    try {
                        ctx.collect(
                                new TestEntry(
                                        j,
                                        Integer.toString(j),
                                        Integer.toString(j),
                                        (double) (j),
                                        j));
                    } catch (Exception e) {
                        if (!ExceptionUtils.findThrowable(e, TestException.class).isPresent()) {
                            LOG.warn("Exception during record emission", e);
                        }
                        throw e;
                    }
                    toAdvance.advance();
                }
            }
            sleep(() -> !lastSnapshotConfirmed);
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
            activeSources.putIfAbsent(
                    getRuntimeContext().getAttemptNumber(),
                    new CountDownLatch(getRuntimeContext().getNumberOfParallelSubtasks()));
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
            LOG.debug("Source initialized with ranges: {}", ranges.get());
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            lastCheckpointId = context.getCheckpointId();
        }

        private void sleep(Supplier<Boolean> condition) {
            long start = System.currentTimeMillis();
            while (condition.get()
                    && running
                    && !Thread.currentThread().isInterrupted()
                    && haveActiveSources()) {
                if (System.currentTimeMillis() - start > 10_000) {
                    // debugging FLINK-22889 (TODO: remove after resolved)
                    LOG.debug("Slept more than 10s", new Exception());
                    start = Long.MAX_VALUE;
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    ExceptionUtils.rethrow(e);
                }
            }
        }

        private void waitOtherSources() throws InterruptedException {
            long start = System.currentTimeMillis();
            while (running && haveActiveSources()) {
                if (System.currentTimeMillis() - start > 10_000) {
                    // debugging FLINK-22889 (TODO: remove after resolved)
                    LOG.debug("Slept more than 10s", new Exception());
                    start = Long.MAX_VALUE;
                }
                activeSources
                        .get(getRuntimeContext().getAttemptNumber())
                        .await(100, TimeUnit.MILLISECONDS);
            }
        }

        private boolean haveActiveSources() {
            return activeSources.get(getRuntimeContext().getAttemptNumber()).getCount() > 0;
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

            @Override
            public String toString() {
                return String.format("%d..%d", from, to);
            }
        }
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
            remaining = minElementsPerFailure + RANDOM.nextInt(maxElementsPerFailure);
            inactiveMappers
                    .computeIfAbsent(
                            getRuntimeContext().getAttemptNumber(),
                            u ->
                                    new CountDownLatch(
                                            getRuntimeContext().getNumberOfParallelSubtasks()))
                    .countDown();
            LOG.debug("Mapper will fail after {} records", remaining);
        }

        @Override
        public TestEntry map(TestEntry value) throws Exception {
            if (--remaining <= 0) {
                LOG.debug("Mapper failing intentionally");
                throw new TestException();
            }
            return value;
        }
    }

    private static final class TestException extends Exception {
        public TestException() {
            // use this string to prevent error parsing scripts from failing the build
            // and still have exception type
            super("java.lang.Exception: Artificial failure", null, true, false);
        }
    }

    private static class MySqlJdbcExactlyOnceSinkTestEnv implements JdbcExactlyOnceSinkTestEnv {
        private final int parallelism;
        private final JdbcDatabaseContainer<?> db;

        public MySqlJdbcExactlyOnceSinkTestEnv(int parallelism) {
            this.parallelism = parallelism;
            this.db = new MySqlXaDb();
        }

        @Override
        public void start() {
            db.start();
        }

        @Override
        public void stop() {
            db.close();
        }

        @Override
        public JdbcDatabaseContainer<?> getContainer() {
            return db;
        }

        @Override
        public SerializableSupplier<XADataSource> getDataSourceSupplier() {
            return new MySqlXaDataSourceFactory(
                    db.getJdbcUrl(), db.getUsername(), db.getPassword());
        }

        @Override
        public int getParallelism() {
            return parallelism;
        }

        private static final class MySqlXaDb extends MySQLContainer<MySqlXaDb> {
            private static final String IMAGE_NAME = "mysql:8.0.23"; // version 5 had issues with XA

            @Override
            public String toString() {
                return IMAGE_NAME;
            }

            public MySqlXaDb() {
                super(IMAGE_NAME);
            }

            @Override
            public void start() {
                super.start();
                // prevent XAER_RMERR: Fatal error occurred in the transaction  branch - check your
                // data for consistency works for mysql v8+
                try (Connection connection =
                        DriverManager.getConnection(getJdbcUrl(), "root", getPassword())) {
                    grantRecover(connection);
                } catch (SQLException e) {
                    ExceptionUtils.rethrow(e);
                }
            }

            private void grantRecover(Connection connection) throws SQLException {
                try (Statement st = connection.createStatement()) {
                    st.execute("GRANT XA_RECOVER_ADMIN ON *.* TO '" + getUsername() + "'@'%'");
                    st.execute("FLUSH PRIVILEGES");
                }
            }
        }

        private static class MySqlXaDataSourceFactory
                implements SerializableSupplier<XADataSource> {
            private final String jdbcUrl;
            private final String username;
            private final String password;

            public MySqlXaDataSourceFactory(String jdbcUrl, String username, String password) {
                this.jdbcUrl = jdbcUrl;
                this.username = username;
                this.password = password;
            }

            @Override
            public XADataSource get() {
                MysqlXADataSource xaDataSource = new MysqlXADataSource();
                xaDataSource.setUrl(jdbcUrl);
                xaDataSource.setUser(username);
                xaDataSource.setPassword(password);
                return xaDataSource;
            }
        }

        @Override
        public String toString() {
            return db + ", parallelism=" + parallelism;
        }
    }

    private static class PgSqlJdbcExactlyOnceSinkTestEnv implements JdbcExactlyOnceSinkTestEnv {
        private final int parallelism;
        private final PgXaDb db;

        private PgSqlJdbcExactlyOnceSinkTestEnv(int parallelism) {
            this.parallelism = parallelism;
            this.db = new PgXaDb(parallelism * 2, 50);
        }

        @Override
        public void start() {
            db.start();
        }

        @Override
        public void stop() {
            db.close();
        }

        @Override
        public JdbcDatabaseContainer<?> getContainer() {
            return db;
        }

        @Override
        public SerializableSupplier<XADataSource> getDataSourceSupplier() {
            return new PgXaDataSourceFactory(db.getJdbcUrl(), db.getUsername(), db.getPassword());
        }

        @Override
        public int getParallelism() {
            return parallelism;
        }

        @Override
        public String toString() {
            return db + ", parallelism=" + parallelism;
        }

        /** {@link PostgreSQLContainer} with XA enabled (by setting max_prepared_transactions). */
        private static final class PgXaDb extends PostgreSQLContainer<PgXaDb> {
            private static final String IMAGE_NAME = "postgres:9.6.12";
            private static final int SUPERUSER_RESERVED_CONNECTIONS = 1;

            @Override
            public String toString() {
                return IMAGE_NAME;
            }

            public PgXaDb(int maxConnections, int maxTransactions) {
                super(IMAGE_NAME);
                checkArgument(
                        maxConnections > SUPERUSER_RESERVED_CONNECTIONS,
                        "maxConnections should be greater than superuser_reserved_connections");
                setCommand(
                        "postgres",
                        "-c",
                        "superuser_reserved_connections=" + SUPERUSER_RESERVED_CONNECTIONS,
                        "-c",
                        "max_connections=" + maxConnections,
                        "-c",
                        "max_prepared_transactions=" + maxTransactions,
                        "-c",
                        "fsync=off");
            }
        }

        private static class PgXaDataSourceFactory implements SerializableSupplier<XADataSource> {
            private final String jdbcUrl;
            private final String username;
            private final String password;

            public PgXaDataSourceFactory(String jdbcUrl, String username, String password) {
                this.jdbcUrl = jdbcUrl;
                this.username = username;
                this.password = password;
            }

            @Override
            public XADataSource get() {
                PGXADataSource xaDataSource = new PGXADataSource();
                xaDataSource.setUrl(jdbcUrl);
                xaDataSource.setUser(username);
                xaDataSource.setPassword(password);
                return xaDataSource;
            }
        }
    }
}
