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

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.python.DataStreamPythonFunctionOperator;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;

/** Base class for all Python DataStream operators executed in embedded Python environment. */
@Internal
public abstract class AbstractEmbeddedDataStreamPythonFunctionOperator<OUT>
        extends AbstractEmbeddedPythonFunctionOperator<OUT>
        implements DataStreamPythonFunctionOperator<OUT> {

    private static final long serialVersionUID = 1L;

    private static final String NUM_PARTITIONS = "NUM_PARTITIONS";

    /** The serialized python function to be executed. */
    private final DataStreamPythonFunctionInfo pythonFunctionInfo;

    private final Map<String, OutputTag<?>> sideOutputTags;

    /** The TypeInformation of output data. */
    protected final TypeInformation<OUT> outputTypeInfo;

    /** The number of partitions for the partition custom function. */
    private Integer numPartitions;

    transient PythonTypeUtils.DataConverter<OUT, Object> outputDataConverter;

    protected transient TimestampedCollector<OUT> collector;

    protected transient boolean hasSideOutput;

    protected transient SideOutputContext sideOutputContext;

    public AbstractEmbeddedDataStreamPythonFunctionOperator(
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
        hasSideOutput = !sideOutputTags.isEmpty();

        if (hasSideOutput) {
            sideOutputContext = new SideOutputContext();
            sideOutputContext.open();
        }

        super.open();

        outputDataConverter =
                PythonTypeUtils.TypeInfoToDataConverter.typeInfoDataConverter(outputTypeInfo);

        collector = new TimestampedCollector<>(output);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return outputTypeInfo;
    }

    @Override
    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public DataStreamPythonFunctionInfo getPythonFunctionInfo() {
        return pythonFunctionInfo;
    }

    @Override
    public void addSideOutputTags(Collection<OutputTag<?>> outputTags) {
        for (OutputTag<?> outputTag : outputTags) {
            sideOutputTags.put(outputTag.getId(), outputTag);
        }
    }

    @Override
    public Collection<OutputTag<?>> getSideOutputTags() {
        return sideOutputTags.values();
    }

    public Map<String, String> getJobParameters() {
        Map<String, String> jobParameters = new HashMap<>();
        if (numPartitions != null) {
            jobParameters.put(NUM_PARTITIONS, String.valueOf(numPartitions));
        }

        KeyedStateBackend<Object> keyedStateBackend = getKeyedStateBackend();
        if (keyedStateBackend != null) {
            jobParameters.put(
                    "inBatchExecutionMode",
                    String.valueOf(inBatchExecutionMode(keyedStateBackend)));
        }
        return jobParameters;
    }

    private class SideOutputContext {

        private Map<String, byte[]> sideOutputTypeInfoPayloads = new HashMap<>();

        private Map<String, PythonTypeUtils.DataConverter<Object, Object>>
                sideOutputDataConverters = new HashMap<>();

        @SuppressWarnings("unchecked")
        public void open() {
            for (Map.Entry<String, OutputTag<?>> entry : sideOutputTags.entrySet()) {
                sideOutputTypeInfoPayloads.put(
                        entry.getKey(), getSideOutputTypeInfoPayload(entry.getValue()));

                sideOutputDataConverters.put(
                        entry.getKey(),
                        PythonTypeUtils.TypeInfoToDataConverter.typeInfoDataConverter(
                                (TypeInformation) entry.getValue().getTypeInfo()));
            }
        }

        @SuppressWarnings("unchecked")
        public void collectSideOutputById(String id, Object record) {
            OutputTag<?> outputTag = sideOutputTags.get(id);
            PythonTypeUtils.DataConverter<Object, Object> sideOutputDataConverter =
                    sideOutputDataConverters.get(id);
            collector.collect(
                    outputTag, new StreamRecord(sideOutputDataConverter.toInternal(record)));
        }

        public Map<String, byte[]> getAllSideOutputTypeInfoPayloads() {
            return sideOutputTypeInfoPayloads;
        }

        private byte[] getSideOutputTypeInfoPayload(OutputTag<?> outputTag) {
            FlinkFnApi.TypeInfo outputTypeInfo =
                    PythonTypeUtils.TypeInfoToProtoConverter.toTypeInfoProto(
                            outputTag.getTypeInfo(), getRuntimeContext().getUserCodeClassLoader());
            return outputTypeInfo.toByteArray();
        }
    }
}
