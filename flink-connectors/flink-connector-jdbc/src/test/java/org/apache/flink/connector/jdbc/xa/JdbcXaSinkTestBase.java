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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.junit.After;
import org.junit.Before;

import javax.sql.XADataSource;
import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcITCase.TEST_ENTRY_JDBC_STATEMENT_BUILDER;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;

/**
 * // todo: javadoc case Base class for {@link JdbcXaSinkFunction} tests. In addition to {@link
 * JdbcTestBase} init it initializes/closes helpers.
 */
public abstract class JdbcXaSinkTestBase extends JdbcTestBase {

    JdbcXaFacadeTestHelper xaHelper;
    JdbcXaSinkTestHelper sinkHelper;
    XADataSource xaDataSource;

    @Before
    public void initHelpers() throws Exception {
        xaDataSource = getDbMetadata().buildXaDataSource();
        xaHelper =
                new JdbcXaFacadeTestHelper(
                        getDbMetadata().buildXaDataSource(),
                        getDbMetadata().getUrl(),
                        INPUT_TABLE,
                        getDbMetadata().getUser(),
                        getDbMetadata().getPassword());
        sinkHelper = buildSinkHelper(createStateHandler());
    }

    private XaSinkStateHandler createStateHandler() {
        return new TestXaSinkStateHandler();
    }

    @After
    public void closeHelpers() throws Exception {
        if (sinkHelper != null) {
            sinkHelper.close();
        }
        if (xaHelper != null) {
            xaHelper.close();
        }
        try (JdbcXaFacadeTestHelper xa =
                new JdbcXaFacadeTestHelper(
                        xaDataSource,
                        getDbMetadata().getUrl(),
                        INPUT_TABLE,
                        getDbMetadata().getUser(),
                        getDbMetadata().getPassword())) {
            xa.cancelAllTx();
        }
    }

    JdbcXaSinkTestHelper buildSinkHelper(XaSinkStateHandler stateHandler) throws Exception {
        return new JdbcXaSinkTestHelper(buildAndInit(0, getXaFacade(), stateHandler), stateHandler);
    }

    private XaFacadeImpl getXaFacade() {
        return XaFacadeImpl.fromXaDataSource(xaDataSource);
    }

    JdbcXaSinkFunction<TestEntry> buildAndInit() throws Exception {
        return buildAndInit(Integer.MAX_VALUE, getXaFacade());
    }

    JdbcXaSinkFunction<TestEntry> buildAndInit(int batchInterval, XaFacade xaFacade)
            throws Exception {
        return buildAndInit(batchInterval, xaFacade, createStateHandler());
    }

    static JdbcXaSinkFunction<TestEntry> buildAndInit(
            int batchInterval, XaFacade xaFacade, XaSinkStateHandler state) throws Exception {
        JdbcXaSinkFunction<TestEntry> sink =
                buildSink(new SemanticXidGenerator(), xaFacade, state, batchInterval);
        sink.initializeState(buildInitCtx(false));
        sink.open(new Configuration());
        return sink;
    }

    static JdbcXaSinkFunction<TestEntry> buildSink(
            XidGenerator xidGenerator,
            XaFacade xaFacade,
            XaSinkStateHandler state,
            int batchInterval) {
        JdbcOutputFormat<TestEntry, TestEntry, JdbcBatchStatementExecutor<TestEntry>> format =
                new JdbcOutputFormat<>(
                        xaFacade,
                        JdbcExecutionOptions.builder().withBatchIntervalMs(batchInterval).build(),
                        ctx ->
                                JdbcBatchStatementExecutor.simple(
                                        String.format(INSERT_TEMPLATE, INPUT_TABLE),
                                        TEST_ENTRY_JDBC_STATEMENT_BUILDER,
                                        Function.identity()),
                        JdbcOutputFormat.RecordExtractor.identity());
        JdbcXaSinkFunction<TestEntry> sink =
                new JdbcXaSinkFunction<>(
                        format,
                        xaFacade,
                        xidGenerator,
                        state,
                        JdbcExactlyOnceOptions.builder().withRecoveredAndRollback(true).build(),
                        new XaGroupOpsImpl(xaFacade));
        sink.setRuntimeContext(TEST_RUNTIME_CONTEXT);
        return sink;
    }

    static final RuntimeContext TEST_RUNTIME_CONTEXT = getRuntimeContext(new JobID());

    static RuntimeContext getRuntimeContext(final JobID jobID) {
        return new RuntimeContext() {

            @Override
            public JobID getJobId() {
                return jobID;
            }

            @Override
            public String getTaskName() {
                return "test";
            }

            @Override
            public OperatorMetricGroup getMetricGroup() {
                return null;
            }

            @Override
            public int getNumberOfParallelSubtasks() {
                return 1;
            }

            @Override
            public int getMaxNumberOfParallelSubtasks() {
                return 1;
            }

            @Override
            public int getIndexOfThisSubtask() {
                return 0;
            }

            @Override
            public int getAttemptNumber() {
                return 0;
            }

            @Override
            public String getTaskNameWithSubtasks() {
                return "test";
            }

            @Override
            public ExecutionConfig getExecutionConfig() {
                return null;
            }

            @Override
            public ClassLoader getUserCodeClassLoader() {
                return null;
            }

            @Override
            public <V, A extends Serializable> void addAccumulator(
                    String name, Accumulator<V, A> accumulator) {}

            @Override
            public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
                return null;
            }

            @Override
            public void registerUserCodeClassLoaderReleaseHookIfAbsent(
                    String releaseHookName, Runnable releaseHook) {
                throw new UnsupportedOperationException();
            }

            @Override
            public IntCounter getIntCounter(String name) {
                return null;
            }

            @Override
            public LongCounter getLongCounter(String name) {
                return null;
            }

            @Override
            public DoubleCounter getDoubleCounter(String name) {
                return null;
            }

            @Override
            public Histogram getHistogram(String name) {
                return null;
            }

            @Override
            public boolean hasBroadcastVariable(String name) {
                return false;
            }

            @Override
            public <RT> List<RT> getBroadcastVariable(String name) {
                return null;
            }

            @Override
            public <T, C> C getBroadcastVariableWithInitializer(
                    String name, BroadcastVariableInitializer<T, C> initializer) {
                return null;
            }

            @Override
            public DistributedCache getDistributedCache() {
                return null;
            }

            @Override
            public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
                return null;
            }

            @Override
            public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
                return null;
            }

            @Override
            public <T> ReducingState<T> getReducingState(
                    ReducingStateDescriptor<T> stateProperties) {
                return null;
            }

            @Override
            public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
                    AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
                return null;
            }

            @Override
            public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <UK, UV> MapState<UK, UV> getMapState(
                    MapStateDescriptor<UK, UV> stateProperties) {
                return null;
            }
        };
    }

    static final SinkFunction.Context TEST_SINK_CONTEXT =
            new SinkFunction.Context() {
                @Override
                public long currentProcessingTime() {
                    return 0;
                }

                @Override
                public long currentWatermark() {
                    return 0;
                }

                @Override
                public Long timestamp() {
                    return 0L;
                }
            };

    static class TestXaSinkStateHandler implements XaSinkStateHandler {
        private static final long serialVersionUID = 1L;

        private JdbcXaSinkFunctionState stored;

        @Override
        public JdbcXaSinkFunctionState load(FunctionInitializationContext context) {
            List<CheckpointAndXid> prepared =
                    stored != null
                            ? stored.getPrepared().stream()
                                    .map(CheckpointAndXid::asRestored)
                                    .collect(Collectors.toList())
                            : Collections.emptyList();
            Collection<Xid> hanging =
                    stored != null ? stored.getHanging() : Collections.emptyList();
            return JdbcXaSinkFunctionState.of(prepared, hanging);
        }

        @Override
        public void store(JdbcXaSinkFunctionState state) {
            stored = state;
        }

        JdbcXaSinkFunctionState get() {
            return stored;
        }

        @Override
        public String toString() {
            return stored == null ? null : stored.toString();
        }
    }

    static StateInitializationContextImpl buildInitCtx(boolean restored) {
        return new StateInitializationContextImpl(
                restored ? 1L : null,
                new DefaultOperatorStateBackend(
                        new ExecutionConfig(),
                        new CloseableRegistry(),
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        null),
                null,
                null,
                null);
    }
}
