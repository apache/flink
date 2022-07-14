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
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.python.Constants.STATEFUL_FUNCTION_URN;
import static org.apache.flink.python.Constants.STATELESS_FUNCTION_URN;

/** Base class for all Python DataStream operators executed in embedded Python environment. */
@Internal
public abstract class AbstractEmbeddedDataStreamPythonFunctionOperator<OUT>
        extends AbstractEmbeddedPythonFunctionOperator<OUT> implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private static final String NUM_PARTITIONS = "NUM_PARTITIONS";

    /** The serialized python function to be executed. */
    private final DataStreamPythonFunctionInfo pythonFunctionInfo;

    /** The TypeInformation of output data. */
    protected final TypeInformation<OUT> outputTypeInfo;

    /** The function urn. */
    protected final String functionUrn;

    /** The number of partitions for the partition custom function. */
    private Integer numPartitions;

    /**
     * Whether it contains partition custom function. If true, the variable numPartitions should be
     * set and the value should be set to the parallelism of the downstream operator.
     */
    private boolean containsPartitionCustom;

    public AbstractEmbeddedDataStreamPythonFunctionOperator(
            String functionUrn,
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<OUT> outputTypeInfo) {
        super(config);
        this.pythonFunctionInfo = Preconditions.checkNotNull(pythonFunctionInfo);
        this.outputTypeInfo = Preconditions.checkNotNull(outputTypeInfo);
        Preconditions.checkArgument(
                functionUrn.equals(STATELESS_FUNCTION_URN)
                        || functionUrn.equals(STATEFUL_FUNCTION_URN),
                "The function urn should be `STATELESS_FUNCTION_URN` or `STATEFUL_FUNCTION_URN`.");
        this.functionUrn = functionUrn;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public void setContainsPartitionCustom(boolean containsPartitionCustom) {
        this.containsPartitionCustom = containsPartitionCustom;
    }

    public boolean containsPartitionCustom() {
        return this.containsPartitionCustom;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return outputTypeInfo;
    }

    @Override
    public PythonEnv getPythonEnv() {
        return pythonFunctionInfo.getPythonFunction().getPythonEnv();
    }

    @Override
    protected void invokeFinishBundle() throws Exception {
        // TODO: Support batches invoking.
    }

    public DataStreamPythonFunctionInfo getPythonFunctionInfo() {
        return pythonFunctionInfo;
    }

    public Map<String, String> getJobParameters() {
        Map<String, String> jobParameters = new HashMap<>();
        if (numPartitions != null) {
            jobParameters.put(NUM_PARTITIONS, String.valueOf(numPartitions));
        }
        return jobParameters;
    }

    public abstract <T> AbstractEmbeddedDataStreamPythonFunctionOperator<T> copy(
            DataStreamPythonFunctionInfo pythonFunctionInfo, TypeInformation<T> outputTypeInfo);
}
