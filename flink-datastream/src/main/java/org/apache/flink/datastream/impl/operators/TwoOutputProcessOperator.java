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

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultRuntimeContext;
import org.apache.flink.datastream.impl.context.DefaultTwoOutputNonPartitionedContext;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/**
 * Operator for {@link TwoOutputStreamProcessFunction}.
 *
 * <p>We support the second output via flink side-output mechanism.
 */
public class TwoOutputProcessOperator<IN, OUT_MAIN, OUT_SIDE>
        extends AbstractUdfStreamOperator<
                OUT_MAIN, TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE>>
        implements OneInputStreamOperator<IN, OUT_MAIN>, BoundedOneInput {
    protected transient TimestampCollector<OUT_MAIN> mainCollector;

    protected transient TimestampCollector<OUT_SIDE> sideCollector;

    protected transient DefaultRuntimeContext context;

    protected transient TwoOutputNonPartitionedContext<OUT_MAIN, OUT_SIDE> nonPartitionedContext;

    protected OutputTag<OUT_SIDE> outputTag;

    public TwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag) {
        super(userFunction);

        this.outputTag = outputTag;
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        this.mainCollector = getMainCollector();
        this.sideCollector = getSideCollector();
        this.context = new DefaultRuntimeContext();
        this.nonPartitionedContext = new DefaultTwoOutputNonPartitionedContext<>();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        mainCollector.setTimestampFromStreamRecord(element);
        sideCollector.setTimestampFromStreamRecord(element);
        userFunction.processRecord(element.getValue(), mainCollector, sideCollector, context);
    }

    @Override
    public void endInput() throws Exception {
        userFunction.endInput(nonPartitionedContext);
    }

    protected TimestampCollector<OUT_MAIN> getMainCollector() {
        return new OutputCollector<>(output);
    }

    public TimestampCollector<OUT_SIDE> getSideCollector() {
        return new SideOutputCollector(output);
    }

    /**
     * This is a special implementation of {@link TimestampCollector} that using side-output
     * mechanism to emit data.
     */
    protected class SideOutputCollector extends TimestampCollector<OUT_SIDE> {
        private final Output<StreamRecord<OUT_MAIN>> output;

        public SideOutputCollector(Output<StreamRecord<OUT_MAIN>> output) {
            this.output = output;
        }

        @Override
        public void collect(OUT_SIDE outputRecord) {
            output.collect(outputTag, reuse.replace(outputRecord));
        }

        @Override
        public void collectAndOverwriteTimestamp(OUT_SIDE record, long timestamp) {
            setTimestamp(timestamp);
            output.collect(outputTag, reuse.replace(record));
        }
    }
}
