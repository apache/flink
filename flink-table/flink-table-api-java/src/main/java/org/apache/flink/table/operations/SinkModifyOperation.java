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
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DML operation that tells to write to a sink.
 *
 * <p>The sink is described by {@link #getContextResolvedTable()}, and in general is used for every
 * sink which implementation is defined with {@link DynamicTableSink}. {@code DataStream} and {@link
 * TableResult#collect()} sinks are handled by respectively {@link ExternalModifyOperation} and
 * {@link CollectModifyOperation}.
 */
@Internal
public class SinkModifyOperation implements ModifyOperation {

    protected final ContextResolvedTable contextResolvedTable;
    private final Map<String, String> staticPartitions;
    private final QueryOperation child;
    private final boolean overwrite;
    private final Map<String, String> dynamicOptions;
    private final ModifyType modifyType;
    @Nullable private final int[][] targetColumns;

    public SinkModifyOperation(ContextResolvedTable contextResolvedTable, QueryOperation child) {
        this(
                contextResolvedTable,
                child,
                Collections.emptyMap(),
                null,
                false,
                Collections.emptyMap());
    }

    public SinkModifyOperation(
            ContextResolvedTable contextResolvedTable,
            QueryOperation child,
            int[][] targetColumns,
            ModifyType modifyType) {
        this(
                contextResolvedTable,
                child,
                Collections.emptyMap(),
                targetColumns,
                false,
                Collections.emptyMap(),
                modifyType);
    }

    public SinkModifyOperation(
            ContextResolvedTable contextResolvedTable,
            QueryOperation child,
            Map<String, String> staticPartitions,
            int[][] targetColumns,
            boolean overwrite,
            Map<String, String> dynamicOptions) {
        this(
                contextResolvedTable,
                child,
                staticPartitions,
                targetColumns,
                overwrite,
                dynamicOptions,
                ModifyType.INSERT);
    }

    public SinkModifyOperation(
            ContextResolvedTable contextResolvedTable,
            QueryOperation child,
            Map<String, String> staticPartitions,
            @Nullable int[][] targetColumns,
            boolean overwrite,
            Map<String, String> dynamicOptions,
            ModifyType modifyType) {
        this.contextResolvedTable = contextResolvedTable;
        this.child = child;
        this.staticPartitions = staticPartitions;
        this.targetColumns = targetColumns;
        this.overwrite = overwrite;
        this.dynamicOptions = dynamicOptions;
        this.modifyType = modifyType;
    }

    public ContextResolvedTable getContextResolvedTable() {
        return contextResolvedTable;
    }

    public Map<String, String> getStaticPartitions() {
        return staticPartitions;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public boolean isUpdate() {
        return modifyType == ModifyType.UPDATE;
    }

    public boolean isDelete() {
        return modifyType == ModifyType.DELETE;
    }

    public Map<String, String> getDynamicOptions() {
        return dynamicOptions;
    }

    @Override
    public QueryOperation getChild() {
        return child;
    }

    /** return null when no column list specified. */
    @Nullable
    public int[][] getTargetColumns() {
        return targetColumns;
    }

    @Override
    public <T> T accept(ModifyOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", getContextResolvedTable().getIdentifier().asSummaryString());
        params.put("modifyType", modifyType);
        params.put("staticPartitions", staticPartitions);
        params.put("targetColumns", targetColumns);
        params.put("overwrite", overwrite);
        params.put("dynamicOptions", dynamicOptions);

        return OperationUtils.formatWithChildren(
                "CatalogSink",
                params,
                Collections.singletonList(child),
                Operation::asSummaryString);
    }

    /** The type of sink modification. */
    @Internal
    public enum ModifyType {
        INSERT,

        UPDATE,

        DELETE
    }
}
