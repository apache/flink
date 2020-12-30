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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecTableSourceScan;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/**
 * Batch {@link ExecNode} to read data from an external source defined by a bounded {@link
 * ScanTableSource}.
 */
public class BatchExecTableSourceScan extends CommonExecTableSourceScan
        implements BatchExecNode<RowData> {

    public BatchExecTableSourceScan(
            ScanTableSource tableSource, RowType outputType, String description) {
        super(tableSource, outputType, description);
    }

    @Override
    public Transformation<RowData> createInputFormatTransformation(
            StreamExecutionEnvironment env,
            InputFormat<RowData, ?> inputFormat,
            InternalTypeInfo<RowData> outputTypeInfo,
            String name) {
        // env.createInput will use ContinuousFileReaderOperator, but it do not support multiple
        // paths. If read partitioned source, after partition pruning, we need let InputFormat
        // to read multiple partitions which are multiple paths.
        // We can use InputFormatSourceFunction directly to support InputFormat.
        InputFormatSourceFunction<RowData> func =
                new InputFormatSourceFunction<>(inputFormat, outputTypeInfo);
        return env.addSource(func, name, outputTypeInfo).getTransformation();
    }
}
