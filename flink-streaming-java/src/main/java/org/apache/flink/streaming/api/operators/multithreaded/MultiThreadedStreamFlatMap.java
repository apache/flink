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

package org.apache.flink.streaming.api.operators.multithreaded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
@Internal
public class MultiThreadedStreamFlatMap<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, FlatMapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    private static final long TERMINATION_TIMEOUT = 5000L;

    private int parallelism;
    private ExecutorService executorService;
    private List<Callable<Void>> tasks;

    public MultiThreadedStreamFlatMap(FlatMapFunction<IN, OUT> flatMapper, int parallelism) {
        super(flatMapper);

        Preconditions.checkArgument(parallelism >= 0 ? true : false, "Invalid parallelism!");

        this.parallelism = parallelism;
        tasks = new ArrayList<>();

        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    // ------------------------------------------------------------------------
    //  operator life cycle
    // ------------------------------------------------------------------------

    @Override
    public void open() throws Exception {
        super.open();
        createExecutorService();
    }

    @Override
    public void close() throws Exception {
        invokeAllTasks();

        closeExecutor();

        super.close();
    }

    @Override
    public void processElement(final StreamRecord<IN> element) throws Exception {
        tasks.add(new ProcessElementTask(userFunction, element, new MultiThreadedTimestampedCollector<>(output)));
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and recovery
    // ------------------------------------------------------------------------

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }

    @Override
    public void restoreState(FSDataInputStream in) throws Exception {
        super.restoreState(in);
    }

    @Override
    public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
        super.notifyOfCompletedCheckpoint(checkpointId);
    }

    // ------------------------------------------------------------------------
    //  Serialization
    // ------------------------------------------------------------------------

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {

    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------
    private void invokeAllTasks() throws InterruptedException {
        executorService.invokeAll(tasks);
    }

    private void createExecutorService() {
        executorService = Executors.newFixedThreadPool(parallelism > 0 ? parallelism : 1);
    }

    private void closeExecutor() {
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS))
                executorService.shutdownNow();
        } catch (InterruptedException interrupted) {
            executorService.shutdownNow();
        }
    }

    private class ProcessElementTask<IN, OUT> implements Callable {
        private FlatMapFunction<IN, OUT> userFunction;
        private StreamRecord<IN> element;
        private TimestampedCollector<OUT> collector;

        public ProcessElementTask(FlatMapFunction<IN, OUT> userFunction,  StreamRecord<IN> element,
                                  TimestampedCollector<OUT> collector) {
            this.userFunction = userFunction;
            this.element = element;
            this.collector = collector;
        }

        @Override
        public Void call() throws Exception {
            processElement();
            return null;
        }

        public void processElement() throws Exception {
            collector.setTimestamp(element);
            userFunction.flatMap(element.getValue(), collector);
        }
    }
}
