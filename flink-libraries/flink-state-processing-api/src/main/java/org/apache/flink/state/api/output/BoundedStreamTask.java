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

package org.apache.flink.state.api.output;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.state.api.functions.Timestamper;
import org.apache.flink.state.api.runtime.NeverFireProcessingTimeService;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactoryUtil;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Optional;

/**
 * A stream task that pulls elements from an {@link Iterable} instead of the network. After all
 * elements are processed the task takes a snapshot of the subtask operator state. This is a shim
 * until stream tasks support bounded inputs.
 *
 * @param <IN> Type of the input.
 * @param <OUT> Type of the output.
 * @param <OP> Type of the operator this task runs.
 */
class BoundedStreamTask<IN, OUT, OP extends OneInputStreamOperator<IN, OUT> & BoundedOneInput>
        extends StreamTask<OUT, OP> {

    private final Iterator<IN> input;

    private final Collector<OUT> collector;

    private final Timestamper<IN> timestamper;

    BoundedStreamTask(
            Environment environment,
            Iterable<IN> input,
            Timestamper<IN> timestamper,
            Collector<OUT> collector)
            throws Exception {
        super(environment, new NeverFireProcessingTimeService());
        this.input = input.iterator();
        this.collector = collector;
        this.timestamper = timestamper;
    }

    @Override
    protected void init() throws Exception {
        Preconditions.checkState(
                operatorChain.getNumberOfOperators() == 1,
                "BoundedStreamTask's should only run a single operator");

        // re-initialize the operator with the correct collector.
        StreamOperatorFactory<OUT> operatorFactory =
                configuration.getStreamOperatorFactory(getUserCodeClassLoader());
        Tuple2<OP, Optional<ProcessingTimeService>> mainOperatorAndTimeService =
                StreamOperatorFactoryUtil.createOperator(
                        operatorFactory,
                        this,
                        configuration,
                        new CollectorWrapper<>(collector),
                        operatorChain.getOperatorEventDispatcher());
        mainOperator = mainOperatorAndTimeService.f0;
        mainOperator.initializeState(createStreamTaskStateInitializer());
        mainOperator.open();
    }

    @Override
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
        if (input.hasNext()) {
            StreamRecord<IN> streamRecord = new StreamRecord<>(input.next());

            if (timestamper != null) {
                long timestamp = timestamper.timestamp(streamRecord.getValue());
                streamRecord.setTimestamp(timestamp);
            }

            mainOperator.setKeyContextElement1(streamRecord);
            mainOperator.processElement(streamRecord);
        } else {
            mainOperator.endInput();
            mainOperator.finish();
            controller.suspendDefaultAction();
            mailboxProcessor.suspend();
        }
    }

    @Override
    protected void cancelTask() {}

    @Override
    protected void cleanUpInternal() throws Exception {
        mainOperator.close();
    }

    private static class CollectorWrapper<OUT> implements Output<StreamRecord<OUT>> {

        private final Collector<OUT> inner;

        private CollectorWrapper(Collector<OUT> inner) {
            this.inner = inner;
        }

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void collect(StreamRecord<OUT> record) {
            inner.collect(record.getValue());
        }

        @Override
        public void close() {}
    }
}
