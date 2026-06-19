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

package org.apache.flink.cep;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Base interface for a pattern select function. A pattern select function is called with a map
 * containing the detected events which can be accessed by their names. The names depend on the
 * definition of the {@link org.apache.flink.cep.pattern.Pattern}. The select method returns exactly
 * one result. If you want to return more than one result, then you have to implement a {@link
 * PatternFlatSelectFunction}.
 *
 * <pre>{@code
 * PatternStream<IN> pattern = ...;
 *
 * DataStream<OUT> result = pattern.select(new MyPatternSelectFunction());
 * }</pre>
 *
 * @param <IN> Type of the input elements
 * @param <OUT> Type of the output element
 */
public interface PatternSelectFunction<IN, OUT> extends Function, Serializable {

    /**
     * Generates a result from the given map of events. The events are identified by their names.
     * Only one resulting element can be generated.
     *
     * @param pattern Map containing the found pattern. Events are identified by their names
     * @return Resulting element
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    OUT select(Map<String, List<IN>> pattern) throws Exception;
}
