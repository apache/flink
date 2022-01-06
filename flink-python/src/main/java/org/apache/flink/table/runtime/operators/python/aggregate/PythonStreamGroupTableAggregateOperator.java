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

package org.apache.flink.table.runtime.operators.python.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.functions.python.PythonAggregateFunctionInfo;
import org.apache.flink.table.runtime.dataview.DataViewSpec;
import org.apache.flink.table.types.logical.RowType;

/** The Python TableAggregateFunction operator. */
@Internal
public class PythonStreamGroupTableAggregateOperator
        extends AbstractPythonStreamGroupAggregateOperator {

    private static final long serialVersionUID = 1L;

    @VisibleForTesting
    protected static final String STREAM_GROUP_TABLE_AGGREGATE_URN =
            "flink:transform:stream_group_table_aggregate:v1";

    public PythonStreamGroupTableAggregateOperator(
            Configuration config,
            RowType inputType,
            RowType outputType,
            PythonAggregateFunctionInfo[] aggregateFunctions,
            DataViewSpec[][] dataViewSpecs,
            int[] grouping,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            long minRetentionTime,
            long maxRetentionTime) {
        super(
                config,
                inputType,
                outputType,
                aggregateFunctions,
                dataViewSpecs,
                grouping,
                indexOfCountStar,
                generateUpdateBefore,
                minRetentionTime,
                maxRetentionTime);
    }

    /**
     * Gets the proto representation of the Python user-defined table aggregate function to be
     * executed.
     */
    @Override
    public FlinkFnApi.UserDefinedAggregateFunctions getUserDefinedFunctionsProto() {
        FlinkFnApi.UserDefinedAggregateFunctions.Builder builder =
                super.getUserDefinedFunctionsProto().toBuilder();
        return builder.build();
    }

    @Override
    public String getFunctionUrn() {
        return STREAM_GROUP_TABLE_AGGREGATE_URN;
    }
}
