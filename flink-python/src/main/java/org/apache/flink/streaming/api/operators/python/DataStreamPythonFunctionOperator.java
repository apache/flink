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
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.util.OutputTag;

import java.util.Collection;

/** Interface for Python DataStream operators. */
@Internal
public interface DataStreamPythonFunctionOperator<OUT> extends ResultTypeQueryable<OUT> {

    /**
     * Sets the number of partitions. This is used for partitionCustom which takes the number of
     * partitions to partition into as input.
     */
    void setNumPartitions(int numPartitions);

    /** Returns the underlying {@link DataStreamPythonFunctionInfo}. */
    DataStreamPythonFunctionInfo getPythonFunctionInfo();

    /** Add a collection of {@link OutputTag}s to the operator. */
    void addSideOutputTags(Collection<OutputTag<?>> outputTags);

    /** Gets the {@link OutputTag}s belongs to the operator. */
    Collection<OutputTag<?>> getSideOutputTags();

    /**
     * Make a copy of the DataStreamPythonFunctionOperator with the given pythonFunctionInfo and
     * outputTypeInfo. This is used for chaining optimization which may need to update the
     * underlying pythonFunctionInfo and outputTypeInfo with the other fields not changed.
     */
    <T> DataStreamPythonFunctionOperator<T> copy(
            DataStreamPythonFunctionInfo pythonFunctionInfo, TypeInformation<T> outputTypeInfo);
}
