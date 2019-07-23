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

package org.apache.flink.table.planner.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.QueryOperationVisitor;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes a relational operation that reads from a {@link DataStream}.
 *
 * <p>This is only used for testing.
 *
 * <p>This operation may expose only part, or change the order of the fields available in a
 * {@link org.apache.flink.api.common.typeutils.CompositeType} of the underlying {@link DataStream}.
 * The {@link DataStreamQueryOperation#getFieldIndices()} describes the mapping between fields of the
 * {@link TableSchema} to the {@link org.apache.flink.api.common.typeutils.CompositeType}.
 */
@Internal
public class DataStreamQueryOperation<E> implements QueryOperation {

	// DataStreamQueryOperation represent a registered DataStream from `registerDataStream` method
	// or a unregistered DataStream from `fromDataStream`. However the name under which the DataStream is registered
	// is missing when converting a DataStreamQueryOperation to a RelNode.
	// so using `qualifiedName` to keep the original name.
	// TODO This is a temporary solution.
	private List<String> qualifiedName;
	private final DataStream<E> dataStream;
	private final int[] fieldIndices;
	private final TableSchema tableSchema;
	// TODO remove this while TableSchema supports fieldNullables
	private final boolean[] fieldNullables;
	private final boolean producesUpdates;
	private final boolean isAccRetract;
	private final FlinkStatistic statistic;

	public DataStreamQueryOperation(
			DataStream<E> dataStream,
			int[] fieldIndices,
			TableSchema tableSchema,
			boolean[] fieldNullables,
			boolean producesUpdates,
			boolean isAccRetract,
			FlinkStatistic statistic) {
		this.dataStream = dataStream;
		this.tableSchema = tableSchema;
		this.fieldNullables = fieldNullables;
		this.fieldIndices = fieldIndices;
		this.producesUpdates = producesUpdates;
		this.isAccRetract = isAccRetract;
		this.statistic = statistic;
	}

	public DataStreamQueryOperation(
			DataStream<E> dataStream,
			int[] fieldIndices,
			TableSchema tableSchema,
			boolean[] fieldNullables,
			FlinkStatistic statistic) {
		this(dataStream, fieldIndices, tableSchema, fieldNullables, false, false, statistic);
	}

	public DataStream<E> getDataStream() {
		return dataStream;
	}

	public int[] getFieldIndices() {
		return fieldIndices;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public String asSummaryString() {
		Map<String, Object> args = new LinkedHashMap<>();
		args.put("id", dataStream.getId());
		args.put("fields", tableSchema.getFieldNames());

		return OperationUtils.formatWithChildren("DataStream", args, getChildren(), Operation::asSummaryString);
	}

	@Override
	public List<QueryOperation> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <T> T accept(QueryOperationVisitor<T> visitor) {
		return visitor.visit(this);
	}

	public List<String> getQualifiedName() {
		return qualifiedName;
	}

	public void setQualifiedName(List<String> qualifiedName) {
		this.qualifiedName = qualifiedName;
	}

	public boolean[] getFieldNullables() {
		return fieldNullables;
	}

	public boolean isProducesUpdates() {
		return producesUpdates;
	}

	public boolean isAccRetract() {
		return isAccRetract;
	}

	public FlinkStatistic getStatistic() {
		return statistic;
	}
}
