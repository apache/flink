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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.AbstractBroadcastStateTransformation;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * A {@link Transformation} representing a Python Keyed-Co-Broadcast-Process operation, which will
 * be translated into different operations by {@link
 * org.apache.flink.streaming.runtime.translators.python.PythonKeyedBroadcastStateTransformationTranslator}.
 */
@Internal
public class PythonKeyedBroadcastStateTransformation<OUT>
        extends AbstractBroadcastStateTransformation<Row, Row, OUT>
        implements DelegateOperatorTransformation<OUT> {

    private final Configuration configuration;
    private final DataStreamPythonFunctionInfo dataStreamPythonFunctionInfo;
    private final TypeInformation<Row> stateKeyType;
    private final KeySelector<Row, Row> keySelector;
    private final SimpleOperatorFactory<OUT> delegateOperatorFactory;

    public PythonKeyedBroadcastStateTransformation(
            String name,
            Configuration configuration,
            DataStreamPythonFunctionInfo dataStreamPythonFunctionInfo,
            Transformation<Row> regularInput,
            Transformation<Row> broadcastInput,
            List<MapStateDescriptor<?, ?>> broadcastStateDescriptors,
            TypeInformation<Row> keyType,
            KeySelector<Row, Row> keySelector,
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
        this.stateKeyType = keyType;
        this.keySelector = keySelector;
        this.delegateOperatorFactory = SimpleOperatorFactory.of(new DelegateOperator<>());
        updateManagedMemoryStateBackendUseCase(true);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public DataStreamPythonFunctionInfo getDataStreamPythonFunctionInfo() {
        return dataStreamPythonFunctionInfo;
    }

    public TypeInformation<Row> getStateKeyType() {
        return stateKeyType;
    }

    public KeySelector<Row, Row> getKeySelector() {
        return keySelector;
    }

    @Override
    public SimpleOperatorFactory<OUT> getOperatorFactory() {
        return delegateOperatorFactory;
    }
}
