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

package org.apache.flink.state.api.output.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * An operator for executing a {@link GroupReduceFunction} on a bounded DataStream.
 *
 * @param <IN> Type of the elements that this function processes.
 * @param <OUT> The type of the elements returned by the user-defined function.
 */
@Internal
public class GroupReduceOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, GroupReduceFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    private transient List<IN> buffer;

    public GroupReduceOperator(GroupReduceFunction<IN, OUT> userFunction) {
        super(userFunction);
        super.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.buffer = new ArrayList<>();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        buffer.add(element.getValue());
    }

    @Override
    public void finish() throws Exception {
        getUserFunction().reduce(buffer, new TimestampedCollector<>(output));
    }
}
