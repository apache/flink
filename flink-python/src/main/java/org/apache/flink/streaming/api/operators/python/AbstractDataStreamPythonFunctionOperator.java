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

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/** Base class for all Python DataStream operators. */
@Internal
public abstract class AbstractDataStreamPythonFunctionOperator<OUT>
        extends AbstractPythonFunctionOperator<OUT> implements ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private static final String NUM_PARTITIONS = "NUM_PARTITIONS";

    /** The number of partitions for the partition custom function. */
    @Nullable private Integer numPartitions = null;

    /**
     * Whether it contains partition custom function. If true, the variable numPartitions should be
     * set and the value should be set to the parallelism of the downstream operator.
     */
    private boolean containsPartitionCustom;

    /** The serialized python function to be executed. */
    private final DataStreamPythonFunctionInfo pythonFunctionInfo;

    /** The TypeInformation of output data. */
    private final TypeInformation<OUT> outputTypeInfo;

    public AbstractDataStreamPythonFunctionOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<OUT> outputTypeInfo) {
        super(config);
        this.pythonFunctionInfo = Preconditions.checkNotNull(pythonFunctionInfo);
        this.outputTypeInfo = Preconditions.checkNotNull(outputTypeInfo);
    }

    @Override
    public PythonEnv getPythonEnv() {
        return pythonFunctionInfo.getPythonFunction().getPythonEnv();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return outputTypeInfo;
    }

    public abstract <T> AbstractDataStreamPythonFunctionOperator<T> copy(
            DataStreamPythonFunctionInfo pythonFunctionInfo, TypeInformation<T> outputTypeInfo);

    public Map<String, String> getInternalParameters() {
        Map<String, String> internalParameters = new HashMap<>();
        if (numPartitions != null) {
            internalParameters.put(NUM_PARTITIONS, String.valueOf(numPartitions));
        }
        return internalParameters;
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

    // ----------------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------------

    public DataStreamPythonFunctionInfo getPythonFunctionInfo() {
        return pythonFunctionInfo;
    }
}
