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

import com.google.common.util.concurrent.*;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;

import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Internal
public class MultiThreadedStreamFlatMap<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, FlatMapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    private static final long TERMINATION_TIMEOUT_IN_DAYS = 365L;
    private static final String STATE_NAME = "_mutlithreaded_flatmap_state_";

    private int parallelism;

    private ListeningExecutorService executorService;

    private transient Object lock;
    private MultiThreadedTimestampedCollector collector;

    private transient StreamElementSerializer<IN> inStreamElementSerializer;
    private transient TypeSerializer<StreamElement> typeSerializer;
    private transient ListState<StreamElement> states;
    private List<StreamElement> elementsToBeProcessedBuffer;

    public MultiThreadedStreamFlatMap(FlatMapFunction<IN, OUT> flatMapper, int parallelism) {
        super(flatMapper);

        Preconditions.checkArgument(parallelism > 0 ? true : false, "Invalid parallelism!");

        this.parallelism = parallelism;

        elementsToBeProcessedBuffer = Collections.synchronizedList(new ArrayList<StreamElement>());
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    // ------------------------------------------------------------------------
    //  operator life cycle
    // ------------------------------------------------------------------------
    @Override
    public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);

        this.inStreamElementSerializer = new StreamElementSerializer<>(
                getOperatorConfig().<IN>getTypeSerializerIn1(getUserCodeClassloader()));
    }

    @Override
    public void open() throws Exception {
        super.open();

        lock = new Object();

        collector = new MultiThreadedTimestampedCollector<>(output, lock);
        createExecutorService();
    }

    @Override
    public void close() throws Exception {
        closeExecutor();

        super.close();
    }

    @Override
    public void processElement(final StreamRecord<IN> element) throws Exception {
        elementsToBeProcessedBuffer.add(element);

        Callable processElementTask =
                new ProcessElementTask(userFunction, element, collector);

        ListenableFuture<StreamRecord<IN>> taskProgress = executorService.submit(processElementTask);

        Futures.addCallback(taskProgress, new TaskProgressCallback());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        states = getOperatorStateBackend().getOperatorState(
                    new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer)
                );

        if (context.isRestored())
            for (StreamElement element : states.get())
                processElement(element.<IN>asRecord());

    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        states.clear();
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and recovery
    // ------------------------------------------------------------------------
    @Override
    public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
        super.snapshotState(out, checkpointId, timestamp);

        DataOutputView outputView = new DataOutputViewStreamWrapper(out);
        outputView.writeInt(elementsToBeProcessedBuffer.size());

        for (StreamElement element: elementsToBeProcessedBuffer)
            typeSerializer.serialize(element, outputView);
    }

    @Override
    public void restoreState(FSDataInputStream state) throws Exception {
        super.restoreState(state);

        final ObjectInputStream objectInputStream = new ObjectInputStream(state);

        int bufferElementsCount = objectInputStream.readInt();

        for (int i = 0; i > bufferElementsCount; i++) {
            StreamRecord<IN> element =
                    typeSerializer.deserialize(new DataInputViewStreamWrapper(objectInputStream)).asRecord();
            processElement(element);
        }
    }

    @Override
    public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
        super.notifyOfCompletedCheckpoint(checkpointId);

        for (StreamElement element : elementsToBeProcessedBuffer)
            states.add(element);
    }

    // ------------------------------------------------------------------------
    //  Serialization
    // ------------------------------------------------------------------------

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        typeSerializer = (TypeSerializer<StreamElement>) type.createSerializer(executionConfig);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------
    private void createExecutorService() {
        executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(parallelism));
    }

    private void closeExecutor() {
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(TERMINATION_TIMEOUT_IN_DAYS, TimeUnit.DAYS))
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
        public StreamRecord<IN> call() throws Exception {
            processElement();

            return element;
        }

        public void processElement() throws Exception {
            collector.setTimestamp(element);
            userFunction.flatMap(element.getValue(), collector);
        }
    }

    private class TaskProgressCallback implements FutureCallback<StreamRecord<IN>> {
        @Override
        public void onSuccess(StreamRecord<IN> record) {
            elementsToBeProcessedBuffer.remove(record);
        }

        @Override
        public void onFailure(Throwable thrown) {
        }
    }
}
