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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.operations.ProjectTableOperation;
import org.apache.flink.table.operations.SetTableOperation;
import org.apache.flink.table.operations.TableOperation;
import org.apache.flink.table.operations.TableOperationDefaultVisitor;
import org.apache.flink.table.operations.TableOperationVisitor;
import org.apache.flink.table.plan.logical.LogicalNode;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Converter from Flink's specific relational representation: {@link TableOperation} to Calcite's specific relational
 * representation: {@link RelNode}.
 */
@Internal
public class TableOperationConverter extends TableOperationDefaultVisitor<RelNode> {

	/**
	 * Supplier for {@link TableOperationConverter} that can wrap given {@link RelBuilder}.
	 */
	@Internal
	public static class ToRelConverterSupplier {
		private final ExpressionBridge<PlannerExpression> expressionBridge;

		public ToRelConverterSupplier(ExpressionBridge<PlannerExpression> expressionBridge) {
			this.expressionBridge = expressionBridge;
		}

		public TableOperationConverter get(RelBuilder relBuilder) {
			return new TableOperationConverter(relBuilder, expressionBridge);
		}
	}

	private final RelBuilder relBuilder;
	private final SingleRelVisitor singleRelVisitor = new SingleRelVisitor();
	private final ExpressionBridge<PlannerExpression> expressionBridge;

	public TableOperationConverter(
			RelBuilder relBuilder,
			ExpressionBridge<PlannerExpression> expressionBridge) {
		this.relBuilder = relBuilder;
		this.expressionBridge = expressionBridge;
	}

	@Override
	public RelNode defaultMethod(TableOperation other) {
		other.getChildren().forEach(child -> relBuilder.push(child.accept(this)));
		return other.accept(singleRelVisitor);
	}

	private class SingleRelVisitor implements TableOperationVisitor<RelNode> {

		@Override
		public RelNode visitProject(ProjectTableOperation projection) {
			List<RexNode> rexNodes = convertToRexNodes(projection.getProjectList());

			return relBuilder.project(
				rexNodes,
				asList(projection.getTableSchema().getFieldNames()),
				true)
				.build();
		}

		@Override
		public RelNode visitSetOperation(SetTableOperation setOperation) {
			switch (setOperation.getType()) {
				case INTERSECT:
					relBuilder.intersect(setOperation.isAll());
					break;
				case MINUS:
					relBuilder.minus(setOperation.isAll());
					break;
				case UNION:
					relBuilder.union(setOperation.isAll());
					break;
			}
			return relBuilder.build();
		}

		@Override
		public RelNode visitOther(TableOperation other) {
			if (other instanceof LogicalNode) {
				return ((LogicalNode) other).toRelNode(relBuilder);
			}

			throw new TableException("Unknown table operation: " + other);
		}

		private List<RexNode> convertToRexNodes(List<Expression> expressions) {
			return expressions
				.stream()
				.map(expressionBridge::bridge)
				.map(expr -> expr.toRexNode(relBuilder))
				.collect(toList());
		}
	}
}
