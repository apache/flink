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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Stream operators can implement this interface if they need access to the output type information
 * at {@code org.apache.flink.streaming.api.graph.StreamGraph} generation. This can be useful for
 * cases where the output type is specified by the returns method and, thus, after the stream
 * operator has been created.
 *
 * <p>NOTE: this class was moved from flink-streaming-java to the same package in flink-core. This
 * was required in order to avoid cyclic dependencies for internal connectors, such as
 * flink-connector-datagen that is used in flink-streaming-java but also relies on {@code
 * OutputTypeConfigurable}. Since flink-core is the dependency of flink-streaming-java, this does
 * not introduce breaking changes.
 */
@PublicEvolving
public interface OutputTypeConfigurable<OUT> {

    /**
     * Is called by the {@code org.apache.flink.streaming.api.graph.StreamGraph#addOperator(Integer,
     * String, StreamOperator, TypeInformation, TypeInformation, String)} method when the {@code
     * org.apache.flink.streaming.api.graph.StreamGraph} is generated. The method is called with the
     * output {@link TypeInformation} which is also used for the {@code
     * org.apache.flink.streaming.runtime.tasks.StreamTask} output serializer.
     *
     * @param outTypeInfo Output type information of the {@code
     *     org.apache.flink.streaming.runtime.tasks.StreamTask}
     * @param executionConfig Execution configuration
     */
    void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig);
}
