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

package org.apache.flink.state.api.input.operator.window;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.state.api.functions.WindowReaderFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collections;

/** A wrapper for reading an evicting window operator with an aggregate function. */
@Internal
public class AggregateEvictingWindowReaderFunction<IN, ACC, R, OUT, KEY, W extends Window>
        extends EvictingWindowReaderFunction<IN, R, OUT, KEY, W> {

    private final AggregateFunction<IN, ACC, R> aggFunction;

    public AggregateEvictingWindowReaderFunction(
            WindowReaderFunction<R, OUT, KEY, W> wrappedFunction,
            AggregateFunction<IN, ACC, R> aggFunction) {
        super(wrappedFunction);
        this.aggFunction = aggFunction;
    }

    @Override
    public Iterable<R> transform(Iterable<StreamRecord<IN>> elements) throws Exception {
        ACC acc = aggFunction.createAccumulator();

        for (StreamRecord<IN> element : elements) {
            acc = aggFunction.add(element.getValue(), acc);
        }

        R result = aggFunction.getResult(acc);
        return Collections.singletonList(result);
    }
}
