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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a relational operation that reads from a {@link DataSet}.
 *
 * <p>This operation may expose only part, or change the order of the fields available in a {@link
 * org.apache.flink.api.common.typeutils.CompositeType} of the underlying {@link DataSet}. The
 * {@link DataSetQueryOperation#getFieldIndices()} describes the mapping between fields of the
 * {@link TableSchema} to the {@link org.apache.flink.api.common.typeutils.CompositeType}.
 */
@Internal
public class DataSetQueryOperation<E> implements QueryOperation {

    private final DataSet<E> dataSet;
    private final int[] fieldIndices;
    private final ResolvedSchema resolvedSchema;

    public DataSetQueryOperation(
            DataSet<E> dataSet, int[] fieldIndices, ResolvedSchema resolvedSchema) {
        this.dataSet = dataSet;
        this.resolvedSchema = resolvedSchema;
        this.fieldIndices = fieldIndices;
    }

    public DataSet<E> getDataSet() {
        return dataSet;
    }

    public int[] getFieldIndices() {
        return fieldIndices;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("fields", resolvedSchema.getColumnNames());

        return OperationUtils.formatWithChildren(
                "DataSet", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
