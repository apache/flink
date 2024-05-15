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

import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.datastream.impl.context.DefaultNonPartitionedContext;
import org.apache.flink.datastream.impl.context.DefaultRuntimeContext;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Operator for {@link OneInputStreamProcessFunction}. */
public class ProcessOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, OneInputStreamProcessFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    protected transient DefaultRuntimeContext context;

    protected transient DefaultNonPartitionedContext<OUT> nonPartitionedContext;

    protected transient TimestampCollector<OUT> outputCollector;

    public ProcessOperator(OneInputStreamProcessFunction<IN, OUT> userFunction) {
        super(userFunction);

        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        context = new DefaultRuntimeContext();
        nonPartitionedContext = new DefaultNonPartitionedContext<>();
        outputCollector = getOutputCollector();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        outputCollector.setTimestampFromStreamRecord(element);
        userFunction.processRecord(element.getValue(), outputCollector, context);
    }

    protected TimestampCollector<OUT> getOutputCollector() {
        return new OutputCollector<>(output);
    }

    @Override
    public void endInput() throws Exception {
        userFunction.endInput(nonPartitionedContext);
    }
}
