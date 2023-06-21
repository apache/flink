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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * DML operation that tells to write to a sink.
 *
 * <p>The sink is described by {@link #getContextResolvedTable()}, and in general is used for every
 * sink which implementation is defined with {@link DynamicTableSink}.
 *
 * <p>StagedSinkModifyOperation is an extension of SinkModifyOperation to the atomic CTAS scenario.
 * By making DynamicTableSink a member variable, we can avoid the second initialization of
 * DynamicTableSink, and more importantly, DynamicTableSink can determine if it is an atomic CTAS
 * operation based on whether applyStaging has been called.
 */
@Internal
public class StagedSinkModifyOperation extends SinkModifyOperation {

    private final DynamicTableSink dynamicTableSink;

    public StagedSinkModifyOperation(
            ContextResolvedTable contextResolvedTable,
            QueryOperation child,
            Map<String, String> staticPartitions,
            int[][] targetColumns,
            boolean overwrite,
            Map<String, String> dynamicOptions,
            DynamicTableSink dynamicTableSink) {
        this(
                contextResolvedTable,
                child,
                staticPartitions,
                targetColumns,
                overwrite,
                dynamicOptions,
                ModifyType.INSERT,
                dynamicTableSink);
    }

    public StagedSinkModifyOperation(
            ContextResolvedTable contextResolvedTable,
            QueryOperation child,
            Map<String, String> staticPartitions,
            @Nullable int[][] targetColumns,
            boolean overwrite,
            Map<String, String> dynamicOptions,
            ModifyType modifyType,
            DynamicTableSink dynamicTableSink) {
        super(
                contextResolvedTable,
                child,
                staticPartitions,
                targetColumns,
                overwrite,
                dynamicOptions,
                modifyType);
        this.dynamicTableSink = dynamicTableSink;
    }

    public DynamicTableSink getDynamicTableSink() {
        return dynamicTableSink;
    }
}
