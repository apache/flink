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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@link PythonPartitionCustomOperator} enables us to set the number of partitions for current
 * operator dynamically when generating the {@link org.apache.flink.streaming.api.graph.StreamGraph}
 * before executing the job. The number of partitions will be set in environment variables for
 * python Worker, so that we can obtain the number of partitions when executing user defined
 * partitioner function.
 */
@Internal
public class PythonPartitionCustomOperator<IN, OUT>
        extends OneInputPythonFunctionOperator<IN, OUT, IN, OUT> {

    private static final long serialVersionUID = 1L;

    private static final String NUM_PARTITIONS = "NUM_PARTITIONS";

    private static final String MAP_CODER_URN = "flink:coder:map:v1";

    private int numPartitions = CoreOptions.DEFAULT_PARALLELISM.defaultValue();

    public PythonPartitionCustomOperator(
            Configuration config,
            TypeInformation<IN> inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo,
            DataStreamPythonFunctionInfo pythonFunctionInfo) {
        super(config, inputTypeInfo, outputTypeInfo, pythonFunctionInfo);
    }

    @Override
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] rawResult = resultTuple.f0;
        int length = resultTuple.f1;
        bais.setBuffer(rawResult, 0, length);
        collector.setAbsoluteTimestamp(bufferedTimestamp.poll());
        collector.collect(runnerOutputTypeSerializer.deserialize(baisWrapper));
    }

    @Override
    public Map<String, String> getInternalParameters() {
        Map<String, String> internalParameters = new HashMap<>();
        internalParameters.put(NUM_PARTITIONS, String.valueOf(this.numPartitions));
        return internalParameters;
    }

    @Override
    public String getCoderUrn() {
        return MAP_CODER_URN;
    }

    public void setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }
}
