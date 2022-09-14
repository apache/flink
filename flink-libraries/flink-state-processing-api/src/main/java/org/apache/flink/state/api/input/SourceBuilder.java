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

package org.apache.flink.state.api.input;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;

/** A utility for constructing {@link InputFormat} based sources that are marked as BOUNDED. */
@Internal
public final class SourceBuilder {

    public static final String SOURCE_NAME = "Custom Source";

    private SourceBuilder() {}

    /**
     * Creates a new source that is bounded.
     *
     * @param env The stream execution environment.
     * @param inputFormat The input source to consume.
     * @param typeInfo The type of the output.
     * @param <OUT> The output type.
     * @return A source that is bounded.
     */
    public static <OUT> DataStreamSource<OUT> fromFormat(
            StreamExecutionEnvironment env,
            InputFormat<OUT, ?> inputFormat,
            TypeInformation<OUT> typeInfo) {
        InputFormatSourceFunction<OUT> function =
                new InputFormatSourceFunction<>(inputFormat, typeInfo);

        env.clean(function);

        final StreamSource<OUT, ?> sourceOperator = new StreamSource<>(function);
        return new DataStreamSource<>(
                env, typeInfo, sourceOperator, true, SOURCE_NAME, Boundedness.BOUNDED);
    }
}
