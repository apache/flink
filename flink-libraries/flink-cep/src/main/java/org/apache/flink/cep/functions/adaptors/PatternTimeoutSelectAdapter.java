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

package org.apache.flink.cep.functions.adaptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adapter that expresses combination of {@link PatternSelectFunction} and {@link
 * PatternTimeoutFlatSelectAdapter} with {@link PatternProcessFunction}.
 */
@Internal
public class PatternTimeoutSelectAdapter<IN, OUT, T> extends PatternSelectAdapter<IN, OUT>
        implements TimedOutPartialMatchHandler<IN> {

    private final PatternTimeoutFunction<IN, T> timeoutFunction;
    private final OutputTag<T> timedOutPartialMatchesTag;

    public PatternTimeoutSelectAdapter(
            final PatternSelectFunction<IN, OUT> selectFunction,
            final PatternTimeoutFunction<IN, T> timeoutFunction,
            final OutputTag<T> timedOutPartialMatchesTag) {
        super(selectFunction);
        this.timeoutFunction = checkNotNull(timeoutFunction);
        this.timedOutPartialMatchesTag = checkNotNull(timedOutPartialMatchesTag);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        FunctionUtils.setFunctionRuntimeContext(timeoutFunction, getRuntimeContext());
        FunctionUtils.openFunction(timeoutFunction, openContext);
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(timeoutFunction);
    }

    @Override
    public void processTimedOutMatch(final Map<String, List<IN>> match, final Context ctx)
            throws Exception {

        final T timedOutPatternResult = timeoutFunction.timeout(match, ctx.timestamp());

        ctx.output(timedOutPartialMatchesTag, timedOutPatternResult);
    }
}
