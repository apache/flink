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

package org.apache.flink.table.expressions;

import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.RexAggLocalVariable;
import org.apache.flink.table.calcite.RexDistinctKeyVariable;
import org.apache.flink.table.type.DecimalType;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.InternalTypes;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.flink.table.calcite.FlinkTypeFactory.toInternalType;
import static org.apache.flink.table.type.TypeConverters.createInternalTypeFromTypeInfo;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isString;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isTemporal;
import static org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval;

/**
 * Visit expression to generator {@link RexNode}.
 */
public class RexNodeGenExpressionVisitor implements ExpressionVisitor<RexNode> {

	private final RelBuilder relBuilder;
	private final FlinkTypeFactory typeFactory;

	public RexNodeGenExpressionVisitor(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
		this.typeFactory = (FlinkTypeFactory) relBuilder.getRexBuilder().getTypeFactory();
	}

	@Override
	public RexNode visitCall(CallExpression call) {
		List<RexNode> child = call.getChildren().stream()
				.map(expression -> expression.accept(RexNodeGenExpressionVisitor.this))
				.collect(Collectors.toList());
		switch (call.getFunctionDefinition().getType()) {
			case SCALAR_FUNCTION:
				return visitScalarFunc(call.getFunctionDefinition(), child);
			default: throw new UnsupportedOperationException();
		}
	}

	private RexNode visitScalarFunc(FunctionDefinition def, List<RexNode> child) {
		switch (def.getName()) {
			// logic functions
			case "ifThenElse":
				return relBuilder.call(SqlStdOperatorTable.CASE, child);

			// comparison functions
			case "isNull":
				return relBuilder.isNull(child.get(0));

			// math functions
			case "plus":
				if (isString(toInternalType(child.get(0).getType()))) {
					return relBuilder.call(
							SqlStdOperatorTable.CONCAT,
							child.get(0),
							relBuilder.cast(child.get(1), VARCHAR));
				} else if (isString(toInternalType(child.get(1).getType()))) {
					return relBuilder.call(
							SqlStdOperatorTable.CONCAT,
							relBuilder.cast(child.get(0), VARCHAR),
							child.get(1));
				} else if (isTimeInterval(toInternalType(child.get(0).getType())) &&
						child.get(0).getType() == child.get(1).getType()) {
					return relBuilder.call(SqlStdOperatorTable.PLUS, child);
				} else if (isTimeInterval(toInternalType(child.get(0).getType()))
						&& isTemporal(toInternalType(child.get(1).getType()))) {
					// Calcite has a bug that can't apply INTERVAL + DATETIME (INTERVAL at left)
					// we manually switch them here
					return relBuilder.call(SqlStdOperatorTable.DATETIME_PLUS, child);
				} else if (isTemporal(toInternalType(child.get(0).getType())) &&
						isTemporal(toInternalType(child.get(1).getType()))) {
					return relBuilder.call(SqlStdOperatorTable.DATETIME_PLUS, child);
				} else {
					return relBuilder.call(SqlStdOperatorTable.PLUS, child);
				}
			case "minus":
				return relBuilder.call(SqlStdOperatorTable.MINUS, child);
			case "equals":
				return relBuilder.call(SqlStdOperatorTable.EQUALS, child);
			case "divide":
				return relBuilder.call(SqlStdOperatorTable.DIVIDE, child);
			default: throw new UnsupportedOperationException(def.getName());
		}
	}

	@Override
	public RexNode visitSymbol(SymbolExpression symbolExpression) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RexNode visitValueLiteral(ValueLiteralExpression expr) {
		InternalType type = createInternalTypeFromTypeInfo(expr.getType());
		Object value = expr.getValue();
		RexBuilder rexBuilder = relBuilder.getRexBuilder();
		FlinkTypeFactory typeFactory = (FlinkTypeFactory) relBuilder.getTypeFactory();
		if (value == null) {
			return relBuilder.getRexBuilder()
					.makeCast(
							typeFactory.createTypeFromInternalType(type, true),
							relBuilder.getRexBuilder().constantNull());
		}

		if (type instanceof DecimalType) {
			DecimalType dt = (DecimalType) type;
			BigDecimal bigDecValue = (BigDecimal) value;
			RelDataType decType = relBuilder.getTypeFactory().createSqlType(SqlTypeName.DECIMAL,
					dt.precision(), dt.scale());
			return relBuilder.getRexBuilder().makeExactLiteral(bigDecValue, decType);
		} else if (InternalTypes.LONG.equals(type)) {
			// create BIGINT literals for long type
			BigDecimal bigint = value instanceof BigDecimal ? (BigDecimal) value : BigDecimal.valueOf((long) value);
			return relBuilder.getRexBuilder().makeBigintLiteral(bigint);
		} else if (InternalTypes.FLOAT.equals(type)) {
			//Float/Double type should be liked as java type here.
			return relBuilder.getRexBuilder().makeApproxLiteral(
					BigDecimal.valueOf(((Number) value).floatValue()),
					relBuilder.getTypeFactory().createSqlType(SqlTypeName.FLOAT));
		} else if (InternalTypes.DOUBLE.equals(type)) {
			//Float/Double type should be liked as java type here.
			return rexBuilder.makeApproxLiteral(
					BigDecimal.valueOf(((Number) value).doubleValue()),
					relBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE));
		} else {
			throw new UnsupportedOperationException(expr.getType() + ":" + expr.getValue());
		}
	}

	@Override
	public RexNode visitFieldReference(FieldReferenceExpression fieldReference) {
		return relBuilder.field(fieldReference.getName());
	}

	@Override
	public RexNode visitTypeLiteral(TypeLiteralExpression typeLiteral) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RexNode visit(Expression other) {
		if (other instanceof ResolvedAggInputReference) {
			return visitResolvedAggInputReference((ResolvedAggInputReference) other);
		} else if (other instanceof ResolvedAggLocalReference) {
			return visitResolvedAggLocalReference((ResolvedAggLocalReference) other);
		} else if (other instanceof ResolvedDistinctKeyReference) {
			return visitResolvedDistinctKeyReference((ResolvedDistinctKeyReference) other);
		} else {
			throw new UnsupportedOperationException(other.getClass().getSimpleName() + ":" + other.toString());
		}
	}

	private RexNode visitResolvedAggInputReference(ResolvedAggInputReference reference) {
		// using index to resolve field directly, name used in toString only
		return new RexInputRef(
				reference.getIndex(),
				typeFactory.createTypeFromInternalType(reference.getResultType(), true));
	}

	private RexNode visitResolvedAggLocalReference(ResolvedAggLocalReference reference) {
		InternalType type = reference.getResultType();
		return new RexAggLocalVariable(
				reference.getFieldTerm(),
				reference.getNullTerm(),
				typeFactory.createTypeFromInternalType(type, true),
				type);
	}

	private RexNode visitResolvedDistinctKeyReference(ResolvedDistinctKeyReference reference) {
		InternalType type = reference.getResultType();
		return new RexDistinctKeyVariable(
				reference.getName(),
				typeFactory.createTypeFromInternalType(type, true),
				type);
	}
}
