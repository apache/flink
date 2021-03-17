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

package org.apache.flink.cep.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.cep.time.TimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * It is called with a map of detected events which are identified by their names. The names are
 * defined by the {@link org.apache.flink.cep.pattern.Pattern} specifying the sought-after pattern.
 * This is the preferred way to process found matches.
 *
 * <pre>{@code
 * PatternStream<IN> pattern = ...
 *
 * DataStream<OUT> result = pattern.process(new MyPatternProcessFunction());
 * }</pre>
 *
 * @param <IN> type of incoming elements
 * @param <OUT> type of produced elements based on found matches
 */
@PublicEvolving
public abstract class PatternProcessFunction<IN, OUT> extends AbstractRichFunction {

    /**
     * Generates resulting elements given a map of detected pattern events. The events are
     * identified by their specified names.
     *
     * <p>{@link PatternProcessFunction.Context#timestamp()} in this case returns the time of the
     * last element that was assigned to the match, resulting in this partial match being finished.
     *
     * @param match map containing the found pattern. Events are identified by their names.
     * @param ctx enables access to time features and emitting results through side outputs
     * @param out Collector used to output the generated elements
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    public abstract void processMatch(
            final Map<String, List<IN>> match, final Context ctx, final Collector<OUT> out)
            throws Exception;

    /**
     * Gives access to time related characteristics as well as enables emitting elements to side
     * outputs.
     */
    public interface Context extends TimeContext {
        /**
         * Emits a record to the side output identified by the {@link OutputTag}.
         *
         * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
         * @param value The record to emit.
         */
        <X> void output(final OutputTag<X> outputTag, final X value);
    }
}
