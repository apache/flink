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

package org.apache.flink.streaming.api.operators.python.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.python.DataStreamPythonFunctionOperator;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.python.util.ProtoUtils.createRawTypeCoderInfoDescriptorProto;

/** Base class for all Python DataStream operators executed in Python process. */
@Internal
public abstract class AbstractExternalDataStreamPythonFunctionOperator<OUT>
        extends AbstractExternalPythonFunctionOperator<OUT>
        implements DataStreamPythonFunctionOperator<OUT> {

    private static final long serialVersionUID = 1L;

    private static final String NUM_PARTITIONS = "NUM_PARTITIONS";

    /** The number of partitions for the partition custom function. */
    @Nullable private Integer numPartitions = null;

    /** The serialized python function to be executed. */
    private final DataStreamPythonFunctionInfo pythonFunctionInfo;

    /** The TypeInformation of output data. */
    private final TypeInformation<OUT> outputTypeInfo;

    private final Map<String, OutputTag<?>> sideOutputTags;

    private transient Map<String, TypeSerializer<Row>> sideOutputSerializers;

    public AbstractExternalDataStreamPythonFunctionOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<OUT> outputTypeInfo) {
        super(config);
        this.pythonFunctionInfo = Preconditions.checkNotNull(pythonFunctionInfo);
        this.outputTypeInfo = Preconditions.checkNotNull(outputTypeInfo);
        this.sideOutputTags = new HashMap<>();
    }

    @Override
    public void open() throws Exception {
        sideOutputSerializers = new HashMap<>();
        for (Map.Entry<String, OutputTag<?>> entry : sideOutputTags.entrySet()) {
            sideOutputSerializers.put(
                    entry.getKey(),
                    PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                            getSideOutputTypeInfo(entry.getValue())));
        }
        super.open();
    }

    @Override
    public PythonEnv getPythonEnv() {
        return pythonFunctionInfo.getPythonFunction().getPythonEnv();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return outputTypeInfo;
    }

    @Override
    public DataStreamPythonFunctionInfo getPythonFunctionInfo() {
        return pythonFunctionInfo;
    }

    @Override
    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public Map<String, String> getInternalParameters() {
        Map<String, String> internalParameters = new HashMap<>();
        if (numPartitions != null) {
            internalParameters.put(NUM_PARTITIONS, String.valueOf(numPartitions));
        }
        return internalParameters;
    }

    // ----------------------------------------------------------------------
    // Side outputs
    // ----------------------------------------------------------------------

    protected OutputTag<?> getOutputTagById(String id) {
        Preconditions.checkArgument(sideOutputTags.containsKey(id));
        return sideOutputTags.get(id);
    }

    public void addSideOutputTags(Collection<OutputTag<?>> outputTags) {
        for (OutputTag<?> outputTag : outputTags) {
            sideOutputTags.put(outputTag.getId(), outputTag);
        }
    }

    public Collection<OutputTag<?>> getSideOutputTags() {
        return sideOutputTags.values();
    }

    protected Map<String, FlinkFnApi.CoderInfoDescriptor> createSideOutputCoderDescriptors() {
        Map<String, FlinkFnApi.CoderInfoDescriptor> descriptorMap = new HashMap<>();
        for (Map.Entry<String, OutputTag<?>> entry : sideOutputTags.entrySet()) {
            descriptorMap.put(
                    entry.getKey(),
                    createRawTypeCoderInfoDescriptorProto(
                            getSideOutputTypeInfo(entry.getValue()),
                            FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE,
                            false,
                            getUserCodeClassloader()));
        }
        return descriptorMap;
    }

    protected TypeSerializer<Row> getSideOutputTypeSerializerById(String id) {
        Preconditions.checkArgument(sideOutputSerializers.containsKey(id));
        return sideOutputSerializers.get(id);
    }

    private static TypeInformation<Row> getSideOutputTypeInfo(OutputTag<?> outputTag) {
        return Types.ROW(Types.LONG, outputTag.getTypeInfo());
    }
}
