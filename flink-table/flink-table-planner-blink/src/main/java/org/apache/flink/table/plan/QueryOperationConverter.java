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

package org.apache.flink.table.plan;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.operations.AggregateQueryOperation;
import org.apache.flink.table.operations.CalculatedQueryOperation;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.DataStreamQueryOperation;
import org.apache.flink.table.operations.DistinctQueryOperation;
import org.apache.flink.table.operations.FilterQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation;
import org.apache.flink.table.operations.PlannerQueryOperation;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.QueryOperationDefaultVisitor;
import org.apache.flink.table.operations.QueryOperationVisitor;
import org.apache.flink.table.operations.RichTableSourceQueryOperation;
import org.apache.flink.table.operations.SetQueryOperation;
import org.apache.flink.table.operations.SortQueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation;
import org.apache.flink.table.plan.schema.DataStreamTable;
import org.apache.flink.table.plan.schema.FlinkRelOptTable;
import org.apache.flink.table.plan.schema.TableSourceTable;
import org.apache.flink.table.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.util.Collections;
import java.util.List;

/**
 * Converter from Flink's specific relational representation: {@link QueryOperation} to Calcite's specific relational
 * representation: {@link RelNode}.
 */
@Internal
public class QueryOperationConverter extends QueryOperationDefaultVisitor<RelNode> {

	private final FlinkRelBuilder relBuilder;
	private final SingleRelVisitor singleRelVisitor = new SingleRelVisitor();

	public QueryOperationConverter(FlinkRelBuilder relBuilder) {
		this.relBuilder = relBuilder;
	}

	@Override
	public RelNode defaultMethod(QueryOperation other) {
		other.getChildren().forEach(child -> relBuilder.push(child.accept(this)));
		return other.accept(singleRelVisitor);
	}

	private class SingleRelVisitor implements QueryOperationVisitor<RelNode> {

		@Override
		public RelNode visit(ProjectQueryOperation projection) {
			throw new UnsupportedOperationException("Unsupported now");
		}

		@Override
		public RelNode visit(AggregateQueryOperation aggregate) {
			throw new UnsupportedOperationException("Unsupported now");
		}

		@Override
		public RelNode visit(WindowAggregateQueryOperation windowAggregate) {
			throw new UnsupportedOperationException("Unsupported now");
		}

		@Override
		public RelNode visit(JoinQueryOperation join) {
			throw new UnsupportedOperationException("Unsupported now");
		}

		@Override
		public RelNode visit(SetQueryOperation setOperation) {
			throw new UnsupportedOperationException("Unsupported now");
		}

		@Override
		public RelNode visit(FilterQueryOperation filter) {
			throw new UnsupportedOperationException("Unsupported now");
		}

		@Override
		public RelNode visit(DistinctQueryOperation distinct) {
			throw new UnsupportedOperationException("Unsupported now");
		}

		@Override
		public RelNode visit(SortQueryOperation sort) {
			throw new UnsupportedOperationException("Unsupported now");
		}

		@Override
		public <U> RelNode visit(CalculatedQueryOperation<U> calculatedTable) {
			throw new UnsupportedOperationException("Unsupported now");
		}

		@Override
		public RelNode visit(CatalogQueryOperation catalogTable) {
			return relBuilder.scan(catalogTable.getTablePath()).build();
		}

		@Override
		public RelNode visit(QueryOperation other) {
			if (other instanceof PlannerQueryOperation) {
				return ((PlannerQueryOperation) other).getCalciteTree();
			} else if (other instanceof DataStreamQueryOperation) {
				return convertToDataStreamScan((DataStreamQueryOperation<?>) other);
			}

			throw new TableException("Unknown table operation: " + other);
		}

		@Override
		public <U> RelNode visit(TableSourceQueryOperation<U> tableSourceOperation) {
			TableSource<?> tableSource = tableSourceOperation.getTableSource();
			boolean isBatch;
			if (tableSource instanceof LookupableTableSource) {
				isBatch = tableSourceOperation.isBatch();
			} else if (tableSource instanceof StreamTableSource) {
				isBatch = ((StreamTableSource<?>) tableSource).isBounded();
			} else {
				throw new TableException(String.format("%s is not supported.", tableSource.getClass().getSimpleName()));
			}

			FlinkStatistic statistic;
			List<String> names;
			if (tableSourceOperation instanceof RichTableSourceQueryOperation &&
				((RichTableSourceQueryOperation<U>) tableSourceOperation).getQualifiedName() != null) {
				statistic = ((RichTableSourceQueryOperation<U>) tableSourceOperation).getStatistic();
				names = ((RichTableSourceQueryOperation<U>) tableSourceOperation).getQualifiedName();
			} else {
				statistic = FlinkStatistic.UNKNOWN();
				// TableSourceScan requires a unique name of a Table for computing a digest.
				// We are using the identity hash of the TableSource object.
				String refId = "Unregistered_TableSource_" + System.identityHashCode(tableSource);
				names = Collections.singletonList(refId);
			}

			TableSourceTable<?> tableSourceTable = new TableSourceTable<>(tableSource, !isBatch, statistic);
			FlinkRelOptTable table = FlinkRelOptTable.create(
				relBuilder.getRelOptSchema(),
				tableSourceTable.getRowType(relBuilder.getTypeFactory()),
				names,
				tableSourceTable);
			return LogicalTableScan.create(relBuilder.getCluster(), table);
		}

		private RelNode convertToDataStreamScan(DataStreamQueryOperation<?> operation) {
			DataStreamTable<?> dataStreamTable = new DataStreamTable<>(
					operation.getDataStream(),
					operation.isProducesUpdates(),
					operation.isAccRetract(),
					operation.getFieldIndices(),
					operation.getTableSchema().getFieldNames(),
					operation.getStatistic(),
					scala.Option.apply(operation.getFieldNullables()));

			List<String> names;
			if (operation.getQualifiedName() != null) {
				names = operation.getQualifiedName();
			} else {
				String refId = String.format("Unregistered_DataStream_%s", operation.getDataStream().getId());
				names = Collections.singletonList(refId);
			}

			FlinkRelOptTable table = FlinkRelOptTable.create(
					relBuilder.getRelOptSchema(),
					dataStreamTable.getRowType(relBuilder.getTypeFactory()),
					names,
					dataStreamTable);
			return LogicalTableScan.create(relBuilder.getCluster(), table);
		}
	}

}
