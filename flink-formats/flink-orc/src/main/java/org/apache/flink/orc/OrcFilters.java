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

package org.apache.flink.orc;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.function.TriFunction;

import org.apache.flink.shaded.curator4.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.function.Function;

/**
 * Utility class that provides helper methods to work with Orc Filter PushDown.
 */
public class OrcFilters {

	private static final Logger LOG = LoggerFactory.getLogger(OrcFileSystemFormatFactory.class);

	private static final ImmutableMap<FunctionDefinition, Function<CallExpression, OrcSplitReader.Predicate>> FILTERS =
			new ImmutableMap.Builder<FunctionDefinition, Function<CallExpression, OrcSplitReader.Predicate>>()
					.put(BuiltInFunctionDefinitions.IS_NULL, OrcFilters::convertIsNull)
					.put(BuiltInFunctionDefinitions.IS_NOT_NULL, OrcFilters::convertIsNotNull)
					.put(BuiltInFunctionDefinitions.NOT, OrcFilters::convertNot)
					.put(BuiltInFunctionDefinitions.OR, OrcFilters::convertOr)
					.put(BuiltInFunctionDefinitions.EQUALS, call -> convertBinary(call, OrcFilters::convertEquals, OrcFilters::convertEquals))
					.put(BuiltInFunctionDefinitions.NOT_EQUALS, call -> convertBinary(call, OrcFilters::convertNotEquals, OrcFilters::convertNotEquals))
					.put(BuiltInFunctionDefinitions.GREATER_THAN, call -> convertBinary(call, OrcFilters::convertGreaterThan, OrcFilters::convertLessThanEquals))
					.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, call -> convertBinary(call, OrcFilters::convertGreaterThanEquals, OrcFilters::convertLessThan))
					.put(BuiltInFunctionDefinitions.LESS_THAN, call -> convertBinary(call, OrcFilters::convertLessThan, OrcFilters::convertGreaterThanEquals))
					.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, call -> convertBinary(call, OrcFilters::convertLessThanEquals, OrcFilters::convertGreaterThan))
					.build();

	private static boolean isRef(Expression expression) {
		return expression instanceof FieldReferenceExpression;
	}

	private static boolean isLit(Expression expression) {
		return expression instanceof ValueLiteralExpression;
	}

	private static boolean isUnaryValid(CallExpression callExpression) {
		return callExpression.getChildren().size() == 1 && isRef(callExpression.getChildren().get(0));
	}

	private static boolean isBinaryValid(CallExpression callExpression) {
		return callExpression.getChildren().size() == 2 && (
				isRef(callExpression.getChildren().get(0)) && isLit(callExpression.getChildren().get(1)) ||
						isLit(callExpression.getChildren().get(0)) && isRef(callExpression.getChildren().get(1))
		);
	}

	private static OrcSplitReader.Predicate convertIsNull(CallExpression callExp) {
		if (!isUnaryValid(callExp)) {
			// not a valid predicate
			LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", callExp);
			return null;
		}

		PredicateLeaf.Type colType = toOrcType(((FieldReferenceExpression) callExp.getChildren().get(0)).getOutputDataType());
		if (colType == null) {
			// unsupported type
			LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", callExp);
			return null;
		}

		String colName = getColumnName(callExp);

		return new OrcSplitReader.IsNull(colName, colType);
	}

	private static OrcSplitReader.Predicate convertIsNotNull(CallExpression callExp) {
		return new OrcSplitReader.Not(convertIsNull(callExp));
	}

	private static OrcSplitReader.Predicate convertNot(CallExpression callExp) {
		if (callExp.getChildren().size() != 1) {
			// not a valid predicate
			LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", callExp);
			return null;
		}

		OrcSplitReader.Predicate c = toOrcPredicate(callExp.getChildren().get(0));
		return c == null ? null : new OrcSplitReader.Not(c);
	}

	private static OrcSplitReader.Predicate convertOr(CallExpression callExp) {
		if (callExp.getChildren().size() < 2) {
			return null;
		}
		Expression left = callExp.getChildren().get(0);
		Expression right = callExp.getChildren().get(1);

		OrcSplitReader.Predicate c1 = toOrcPredicate(left);
		OrcSplitReader.Predicate c2 = toOrcPredicate(right);
		if (c1 == null || c2 == null) {
			return null;
		} else {
			return new OrcSplitReader.Or(c1, c2);
		}
	}

	public static OrcSplitReader.Predicate convertBinary(CallExpression callExp,
			TriFunction<String, PredicateLeaf.Type, Serializable, OrcSplitReader.Predicate> func,
			TriFunction<String, PredicateLeaf.Type, Serializable, OrcSplitReader.Predicate> reverseFunc) {
		if (!isBinaryValid(callExp)) {
			// not a valid predicate
			LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", callExp);
			return null;
		}

		PredicateLeaf.Type litType = getLiteralType(callExp);
		if (litType == null) {
			// unsupported literal type
			LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", callExp);
			return null;
		}

		String colName = getColumnName(callExp);

		// fetch literal and ensure it is serializable
		Object literalObj = getLiteral(callExp).get();
		Object orcObj = toOrcObject(litType, literalObj);
		Serializable literal;
		// validate that literal is serializable
		if (orcObj instanceof Serializable) {
			literal = (Serializable) orcObj;
		} else {
			LOG.warn("Encountered a non-serializable literal of type {}. " +
							"Cannot push predicate [{}] into OrcFileSystemFormatFactory. " +
							"This is a bug and should be reported.",
					literalObj.getClass().getCanonicalName(), callExp);
			return null;
		}

		return literalOnRight(callExp) ? func.apply(colName, litType, literal) : reverseFunc.apply(colName, litType, literal);
	}

	private static OrcSplitReader.Predicate convertEquals(String colName, PredicateLeaf.Type litType, Serializable literal) {
		return new OrcSplitReader.Equals(colName, litType, literal);
	}

	private static OrcSplitReader.Predicate convertNotEquals(String colName, PredicateLeaf.Type litType, Serializable literal) {
		return new OrcSplitReader.Not(convertEquals(colName, litType, literal));
	}

	private static OrcSplitReader.Predicate convertGreaterThan(String colName, PredicateLeaf.Type litType, Serializable literal) {
		return new OrcSplitReader.Not(
				new OrcSplitReader.LessThanEquals(colName, litType, literal));
	}

	private static OrcSplitReader.Predicate convertGreaterThanEquals(String colName, PredicateLeaf.Type litType, Serializable literal) {
		return new OrcSplitReader.Not(
				new OrcSplitReader.LessThan(colName, litType, literal));
	}

	private static OrcSplitReader.Predicate convertLessThan(String colName, PredicateLeaf.Type litType, Serializable literal) {
		return new OrcSplitReader.LessThan(colName, litType, literal);
	}

	private static OrcSplitReader.Predicate convertLessThanEquals(String colName, PredicateLeaf.Type litType, Serializable literal) {
		return new OrcSplitReader.LessThanEquals(colName, litType, literal);
	}

	public static OrcSplitReader.Predicate toOrcPredicate(Expression expression) {
		if (expression instanceof CallExpression) {
			CallExpression callExp = (CallExpression) expression;
			if (FILTERS.get(callExp.getFunctionDefinition()) == null) {
				// unsupported predicate
				LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", expression);
				return null;
			}
			return FILTERS.get(callExp.getFunctionDefinition()).apply(callExp);
		} else {
			// unsupported predicate
			LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", expression);
			return null;
		}
	}

	private static String getColumnName(CallExpression comp) {
		if (literalOnRight(comp)) {
			return ((FieldReferenceExpression) comp.getChildren().get(0)).getName();
		} else {
			return ((FieldReferenceExpression) comp.getChildren().get(1)).getName();
		}
	}

	private static boolean literalOnRight(CallExpression comp) {
		if (comp.getChildren().size() == 1 && comp.getChildren().get(0) instanceof FieldReferenceExpression) {
			return true;
		} else if (isLit(comp.getChildren().get(0)) && isRef(comp.getChildren().get(1))) {
			return false;
		} else if (isRef(comp.getChildren().get(0)) && isLit(comp.getChildren().get(1))) {
			return true;
		} else {
			throw new RuntimeException("Invalid binary comparison.");
		}
	}

	private static PredicateLeaf.Type getLiteralType(CallExpression comp) {
		if (literalOnRight(comp)) {
			return toOrcType(((ValueLiteralExpression) comp.getChildren().get(1)).getOutputDataType());
		} else {
			return toOrcType(((ValueLiteralExpression) comp.getChildren().get(0)).getOutputDataType());
		}
	}

	private static Object toOrcObject(PredicateLeaf.Type litType, Object literalObj){
		switch (litType){
			case DATE:
				if (literalObj instanceof LocalDate){
					LocalDate localDate = (LocalDate) literalObj;
					return Date.valueOf(localDate);
				} else {
					return literalObj;
				}
			case TIMESTAMP:
				if (literalObj instanceof LocalDateTime){
					LocalDateTime localDateTime = (LocalDateTime) literalObj;
					return Timestamp.valueOf(localDateTime);
				} else {
					return literalObj;
				}
			default:
				return literalObj;
		}
	}

	private static Optional<?> getLiteral(CallExpression comp) {
		if (literalOnRight(comp)) {
			ValueLiteralExpression valueLiteralExpression = (ValueLiteralExpression) comp.getChildren().get(1);
			return valueLiteralExpression.getValueAs(valueLiteralExpression.getOutputDataType().getConversionClass());
		} else {
			ValueLiteralExpression valueLiteralExpression = (ValueLiteralExpression) comp.getChildren().get(0);
			return valueLiteralExpression.getValueAs(valueLiteralExpression.getOutputDataType().getConversionClass());
		}
	}

	private static PredicateLeaf.Type toOrcType(DataType type) {
		LogicalTypeRoot ltype = type.getLogicalType().getTypeRoot();
		switch (ltype) {
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
				return PredicateLeaf.Type.LONG;
			case FLOAT:
			case DOUBLE:
				return PredicateLeaf.Type.FLOAT;
			case BOOLEAN:
				return PredicateLeaf.Type.BOOLEAN;
			case CHAR:
			case VARCHAR:
				return PredicateLeaf.Type.STRING;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return PredicateLeaf.Type.TIMESTAMP;
			case DATE:
				return PredicateLeaf.Type.DATE;
			case DECIMAL:
				return PredicateLeaf.Type.DECIMAL;
			default:
				return null;
		}
	}
}
