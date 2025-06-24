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
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Adapter that expresses {@link PatternSelectFunction} with {@link PatternProcessFunction}. */
@Internal
public class PatternSelectAdapter<IN, OUT> extends PatternProcessFunction<IN, OUT> {

    private final PatternSelectFunction<IN, OUT> selectFunction;

    public PatternSelectAdapter(final PatternSelectFunction<IN, OUT> selectFunction) {
        this.selectFunction = checkNotNull(selectFunction);
    }

    @Override
    public void open(final OpenContext openContext) throws Exception {
        FunctionUtils.setFunctionRuntimeContext(selectFunction, getRuntimeContext());
        FunctionUtils.openFunction(selectFunction, openContext);
    }

    @Override
    public void close() throws Exception {
        FunctionUtils.closeFunction(selectFunction);
    }

    @Override
    public void processMatch(
            final Map<String, List<IN>> match, final Context ctx, final Collector<OUT> out)
            throws Exception {
        out.collect(selectFunction.select(match));
    }
}
