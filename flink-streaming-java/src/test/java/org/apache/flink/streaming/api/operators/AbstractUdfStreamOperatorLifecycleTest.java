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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** This test secures the lifecycle of AbstractUdfStreamOperator, including it's UDF handling. */
public class AbstractUdfStreamOperatorLifecycleTest {

    private static final List<String> EXPECTED_CALL_ORDER_FULL =
            Arrays.asList(
                    "OPERATOR::setup",
                    "UDF::setRuntimeContext",
                    "OPERATOR::initializeState",
                    "OPERATOR::open",
                    "UDF::open",
                    "OPERATOR::run",
                    "UDF::run",
                    "OPERATOR::prepareSnapshotPreBarrier",
                    "OPERATOR::snapshotState",
                    "OPERATOR::close",
                    "UDF::close",
                    "OPERATOR::dispose");

    private static final List<String> EXPECTED_CALL_ORDER_CANCEL_RUNNING =
            Arrays.asList(
                    "OPERATOR::setup",
                    "UDF::setRuntimeContext",
                    "OPERATOR::initializeState",
                    "OPERATOR::open",
                    "UDF::open",
                    "OPERATOR::run",
                    "UDF::run",
                    "OPERATOR::cancel",
                    "UDF::cancel",
                    "OPERATOR::dispose",
                    "UDF::close");

    private static final String ALL_METHODS_STREAM_OPERATOR =
            "["
                    + "close[], "
                    + "dispose[], "
                    + "getCurrentKey[], "
                    + "getMetricGroup[], "
                    + "getOperatorID[], "
                    + "initializeState[interface org.apache.flink.streaming.api.operators.StreamTaskStateInitializer], "
                    + "notifyCheckpointAborted[long], "
                    + "notifyCheckpointComplete[long], "
                    + "open[], "
                    + "prepareSnapshotPreBarrier[long], "
                    + "setCurrentKey[class java.lang.Object], "
                    + "setKeyContextElement1[class org.apache.flink.streaming.runtime.streamrecord.StreamRecord], "
                    + "setKeyContextElement2[class org.apache.flink.streaming.runtime.streamrecord.StreamRecord], "
                    + "snapshotState[long, long, class org.apache.flink.runtime.checkpoint.CheckpointOptions, interface org.apache.flink.runtime.state.CheckpointStreamFactory]]";

    private static final String ALL_METHODS_RICH_FUNCTION =
            "[close[], getIterationRuntimeContext[], getRuntimeContext[]"
                    + ", open[class org.apache.flink.configuration.Configuration], setRuntimeContext[interface "
                    + "org.apache.flink.api.common.functions.RuntimeContext]]";

    private static final List<String> ACTUAL_ORDER_TRACKING =
            Collections.synchronizedList(new ArrayList<String>(EXPECTED_CALL_ORDER_FULL.size()));

    @Test
    public void testAllMethodsRegisteredInTest() {
        List<String> methodsWithSignatureString = new ArrayList<>();
        for (Method method : StreamOperator.class.getMethods()) {
            methodsWithSignatureString.add(
                    method.getName() + Arrays.toString(method.getParameterTypes()));
        }
        Collections.sort(methodsWithSignatureString);
        Assert.assertEquals(
                "It seems like new methods have been introduced to "
                        + StreamOperator.class
                        + ". Please register them with this test and ensure to document their position in the lifecycle "
                        + "(if applicable).",
                ALL_METHODS_STREAM_OPERATOR,
                methodsWithSignatureString.toString());

        methodsWithSignatureString = new ArrayList<>();
        for (Method method : RichFunction.class.getMethods()) {
            methodsWithSignatureString.add(
                    method.getName() + Arrays.toString(method.getParameterTypes()));
        }
        Collections.sort(methodsWithSignatureString);
        Assert.assertEquals(
                "It seems like new methods have been introduced to "
                        + RichFunction.class
                        + ". Please register them with this test and ensure to document their position in the lifecycle "
                        + "(if applicable).",
                ALL_METHODS_RICH_FUNCTION,
                methodsWithSignatureString.toString());
    }

    @Test
    public void testLifeCycleFull() throws Exception {
        ACTUAL_ORDER_TRACKING.clear();

        Configuration taskManagerConfig = new Configuration();
        StreamConfig cfg = new StreamConfig(new Configuration());
        MockSourceFunction srcFun = new MockSourceFunction();

        cfg.setStreamOperator(new LifecycleTrackingStreamSource<>(srcFun, true));
        cfg.setOperatorID(new OperatorID());
        cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        try (ShuffleEnvironment shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build()) {
            Task task =
                    StreamTaskTest.createTask(
                            SourceStreamTask.class, shuffleEnvironment, cfg, taskManagerConfig);

            task.startTaskThread();

            LifecycleTrackingStreamSource.runStarted.await();

            // wait for clean termination
            task.getExecutingThread().join();
            assertEquals(ExecutionState.FINISHED, task.getExecutionState());
            assertEquals(EXPECTED_CALL_ORDER_FULL, ACTUAL_ORDER_TRACKING);
        }
    }

    @Test
    public void testLifeCycleCancel() throws Exception {
        ACTUAL_ORDER_TRACKING.clear();

        Configuration taskManagerConfig = new Configuration();
        StreamConfig cfg = new StreamConfig(new Configuration());
        MockSourceFunction srcFun = new MockSourceFunction();
        cfg.setStreamOperator(new LifecycleTrackingStreamSource<>(srcFun, false));
        cfg.setOperatorID(new OperatorID());
        cfg.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        try (ShuffleEnvironment shuffleEnvironment = new NettyShuffleEnvironmentBuilder().build()) {
            Task task =
                    StreamTaskTest.createTask(
                            SourceStreamTask.class, shuffleEnvironment, cfg, taskManagerConfig);

            task.startTaskThread();
            LifecycleTrackingStreamSource.runStarted.await();

            // this should cancel the task even though it is blocked on runFinished
            task.cancelExecution();

            // wait for clean termination
            task.getExecutingThread().join();
            assertEquals(ExecutionState.CANCELED, task.getExecutionState());
            assertEquals(EXPECTED_CALL_ORDER_CANCEL_RUNNING, ACTUAL_ORDER_TRACKING);
        }
    }

    private static class MockSourceFunction extends RichSourceFunction<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public void run(SourceContext<Long> ctx) {
            ACTUAL_ORDER_TRACKING.add("UDF::run");
        }

        @Override
        public void cancel() {
            ACTUAL_ORDER_TRACKING.add("UDF::cancel");
        }

        @Override
        public void setRuntimeContext(RuntimeContext t) {
            ACTUAL_ORDER_TRACKING.add("UDF::setRuntimeContext");
            super.setRuntimeContext(t);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ACTUAL_ORDER_TRACKING.add("UDF::open");
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            ACTUAL_ORDER_TRACKING.add("UDF::close");
            super.close();
        }
    }

    private static class LifecycleTrackingStreamSource<OUT, SRC extends SourceFunction<OUT>>
            extends StreamSource<OUT, SRC> implements Serializable {

        private static final long serialVersionUID = 2431488948886850562L;
        private transient Thread testCheckpointer;

        private final boolean simulateCheckpointing;

        static OneShotLatch runStarted;
        static OneShotLatch runFinish;

        public LifecycleTrackingStreamSource(SRC sourceFunction, boolean simulateCheckpointing) {
            super(sourceFunction);
            this.simulateCheckpointing = simulateCheckpointing;
            runStarted = new OneShotLatch();
            runFinish = new OneShotLatch();
        }

        @Override
        public void run(
                Object lockingObject,
                StreamStatusMaintainer streamStatusMaintainer,
                Output<StreamRecord<OUT>> collector,
                OperatorChain<?, ?> operatorChain)
                throws Exception {
            ACTUAL_ORDER_TRACKING.add("OPERATOR::run");
            super.run(lockingObject, streamStatusMaintainer, collector, operatorChain);
            runStarted.trigger();
            runFinish.await();
        }

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<OUT>> output) {
            ACTUAL_ORDER_TRACKING.add("OPERATOR::setup");
            super.setup(containingTask, config, output);
            if (simulateCheckpointing) {
                testCheckpointer =
                        new Thread() {
                            @Override
                            public void run() {
                                try {
                                    runStarted.await();
                                    if (getContainingTask().isCanceled()
                                            || getContainingTask()
                                                    .triggerCheckpointAsync(
                                                            new CheckpointMetaData(
                                                                    0, System.currentTimeMillis()),
                                                            CheckpointOptions
                                                                    .forCheckpointWithDefaultLocation())
                                                    .get()) {
                                        LifecycleTrackingStreamSource.runFinish.trigger();
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    Assert.fail();
                                }
                            }
                        };
                testCheckpointer.start();
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            ACTUAL_ORDER_TRACKING.add("OPERATOR::snapshotState");
            super.snapshotState(context);
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            ACTUAL_ORDER_TRACKING.add("OPERATOR::initializeState");
            super.initializeState(context);
        }

        @Override
        public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
            ACTUAL_ORDER_TRACKING.add("OPERATOR::prepareSnapshotPreBarrier");
            super.prepareSnapshotPreBarrier(checkpointId);
        }

        @Override
        public void open() throws Exception {
            ACTUAL_ORDER_TRACKING.add("OPERATOR::open");
            super.open();
        }

        @Override
        public void close() throws Exception {
            ACTUAL_ORDER_TRACKING.add("OPERATOR::close");
            super.close();
        }

        @Override
        public void cancel() {
            ACTUAL_ORDER_TRACKING.add("OPERATOR::cancel");
            super.cancel();
        }

        @Override
        public void dispose() throws Exception {
            ACTUAL_ORDER_TRACKING.add("OPERATOR::dispose");
            super.dispose();
            if (simulateCheckpointing) {
                testCheckpointer.join();
            }
        }
    }
}
