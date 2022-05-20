/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations.python;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.transformations.AbstractBroadcastStateTransformation;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;
import org.apache.flink.streaming.api.utils.ByteArrayWrapperSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link Transformation} representing a Python Co-Broadcast-Process operation, which will be
 * translated into different operations by {@link
 * org.apache.flink.streaming.runtime.translators.python.PythonBroadcastStateTransformationTranslator}.
 */
public class PythonBroadcastStateTransformation<IN1, IN2, OUT>
        extends AbstractBroadcastStateTransformation<IN1, IN2, OUT> {

    private final Configuration configuration;
    private final DataStreamPythonFunctionInfo dataStreamPythonFunctionInfo;

    public PythonBroadcastStateTransformation(
            String name,
            Configuration configuration,
            DataStreamPythonFunctionInfo dataStreamPythonFunctionInfo,
            Transformation<IN1> regularInput,
            Transformation<IN2> broadcastInput,
            List<MapStateDescriptor<?, ?>> broadcastStateDescriptors,
            TypeInformation<OUT> outTypeInfo,
            int parallelism) {
        super(
                name,
                regularInput,
                broadcastInput,
                broadcastStateDescriptors,
                outTypeInfo,
                parallelism);
        this.configuration = configuration;
        this.dataStreamPythonFunctionInfo = dataStreamPythonFunctionInfo;
        updateManagedMemoryStateBackendUseCase(false);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public DataStreamPythonFunctionInfo getDataStreamPythonFunctionInfo() {
        return dataStreamPythonFunctionInfo;
    }

    public static List<MapStateDescriptor<ByteArrayWrapper, byte[]>>
            convertStateNamesToStateDescriptors(Collection<String> names) {
        List<MapStateDescriptor<ByteArrayWrapper, byte[]>> descriptors =
                new ArrayList<>(names.size());
        TypeSerializer<byte[]> byteArraySerializer =
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO.createSerializer(
                        new ExecutionConfig());
        for (String name : names) {
            descriptors.add(
                    new MapStateDescriptor<>(
                            name, ByteArrayWrapperSerializer.INSTANCE, byteArraySerializer));
        }
        return descriptors;
    }
}
