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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.flink.shaded.curator4.com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.Function;

/**
 * Utility class that provides helper methods to work with Orc Filter PushDown.
 */
public class OrcFilters {

	private static final Logger LOG = LoggerFactory.getLogger(OrcFileSystemFormatFactory.class);

	private static final ImmutableMap<FunctionDefinition, Function<CallExpression, OrcSplitReader.Predicate>> FILTERS =
			new ImmutableMap.Builder<FunctionDefinition, Function<CallExpression, OrcSplitReader.Predicate>>()
					.put(BuiltInFunctionDefinitions.IS_NULL, createIsNullPredicateConverter())
					.put(BuiltInFunctionDefinitions.IS_NOT_NULL, createIsNotNullPredicateConverter())
					.put(BuiltInFunctionDefinitions.NOT, createNotPredicateConverter())
					.put(BuiltInFunctionDefinitions.OR, createOrPredicateConverter())
					.put(BuiltInFunctionDefinitions.EQUALS, createEqualsPredicateConverter())
					.put(BuiltInFunctionDefinitions.NOT_EQUALS, createNotEqualsPredicateConverter())
					.put(BuiltInFunctionDefinitions.GREATER_THAN, createGreaterThanPredicateConverter())
					.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, createGreaterThanEqualsPredicateConverter())
					.put(BuiltInFunctionDefinitions.LESS_THAN, createLessThanPredicateConverter())
					.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, createLessThanEqualsPredicateConverter())
					.build();

	private static boolean isUnaryValid(CallExpression callExpression) {
		return callExpression.getChildren().size() == 1 && callExpression.getChildren().get(0) instanceof FieldReferenceExpression;
	}

	private static boolean isBinaryValid(CallExpression callExpression) {
		return callExpression.getChildren().size() == 2 && ((callExpression.getChildren().get(0) instanceof FieldReferenceExpression && callExpression.getChildren().get(1) instanceof ValueLiteralExpression) ||
				(callExpression.getChildren().get(0) instanceof ValueLiteralExpression && callExpression.getChildren().get(1) instanceof FieldReferenceExpression));
	}

	private static Tuple2<String, PredicateLeaf.Type> getTuple2Args(CallExpression callExp){
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

		return Tuple2.of(colName, colType);
	}

	private static Tuple3<String, PredicateLeaf.Type, Serializable> getTuple3Args(CallExpression callExp){
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
		Serializable literal;
		// validate that literal is serializable
		if (literalObj instanceof Serializable) {
			literal = (Serializable) literalObj;
		} else {
			LOG.warn("Encountered a non-serializable literal of type {}. " +
							"Cannot push predicate [{}] into OrcFileSystemFormatFactory. " +
							"This is a bug and should be reported.",
					literalObj.getClass().getCanonicalName(), callExp);
			return null;
		}

		return Tuple3.of(colName, litType, literal);
	}

	private static Function<CallExpression, OrcSplitReader.Predicate> createIsNullPredicateConverter(){
		return callExp -> {
				Tuple2<String, PredicateLeaf.Type> tuple2 = getTuple2Args(callExp);

				if (tuple2 == null){
					return null;
				}

				return new OrcSplitReader.IsNull(tuple2.f0, tuple2.f1);
		};
	}

	private static Function<CallExpression, OrcSplitReader.Predicate> createIsNotNullPredicateConverter(){
		return callExp -> {

			Tuple2<String, PredicateLeaf.Type> tuple2 = getTuple2Args(callExp);

			if (tuple2 == null){
				return null;
			}

			return new OrcSplitReader.Not(
					new OrcSplitReader.IsNull(tuple2.f0, tuple2.f1));
		};
	}

	private static Function<CallExpression, OrcSplitReader.Predicate> createNotPredicateConverter(){
		return callExp -> {
			if (callExp.getChildren().size() != 1) {
				// not a valid predicate
				LOG.debug("Unsupported predicate [{}] cannot be pushed into OrcFileSystemFormatFactory.", callExp);
				return null;
			}

			OrcSplitReader.Predicate c = toOrcPredicate(callExp.getChildren().get(0));
			if (c == null) {
				return null;
			} else {
				return new OrcSplitReader.Not(c);
			}
		};
	}

	private static Function<CallExpression, OrcSplitReader.Predicate> createOrPredicateConverter(){
		return callExp -> {
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
		};
	}

	private static Function<CallExpression, OrcSplitReader.Predicate> createEqualsPredicateConverter(){
		return callExp -> {
			Tuple3<String, PredicateLeaf.Type, Serializable> tuple3 = getTuple3Args(callExp);

			if (tuple3 == null){
				return null;
			}
			return new OrcSplitReader.Equals(tuple3.f0, tuple3.f1, tuple3.f2);
		};
	}

	private static Function<CallExpression, OrcSplitReader.Predicate> createNotEqualsPredicateConverter(){
		return callExp -> {
			Tuple3<String, PredicateLeaf.Type, Serializable> tuple3 = getTuple3Args(callExp);

			if (tuple3 == null){
				return null;
			}

			return new OrcSplitReader.Not(
					new OrcSplitReader.Equals(tuple3.f0, tuple3.f1, tuple3.f2));
		};
	}

	private static Function<CallExpression, OrcSplitReader.Predicate> createGreaterThanPredicateConverter(){
		return callExp -> {

			Tuple3<String, PredicateLeaf.Type, Serializable> tuple3 = getTuple3Args(callExp);

			if (tuple3 == null){
				return null;
			}

			boolean literalOnRight = literalOnRight(callExp);

			if (literalOnRight) {
				return new OrcSplitReader.Not(
						new OrcSplitReader.LessThanEquals(tuple3.f0, tuple3.f1, tuple3.f2));
			} else {
				return new OrcSplitReader.LessThan(tuple3.f0, tuple3.f1, tuple3.f2);
			}
		};
	}

	private static Function<CallExpression, OrcSplitReader.Predicate> createGreaterThanEqualsPredicateConverter(){
		return callExp -> {

			Tuple3<String, PredicateLeaf.Type, Serializable> tuple3 = getTuple3Args(callExp);

			if (tuple3 == null){
				return null;
			}

			boolean literalOnRight = literalOnRight(callExp);

			if (literalOnRight) {
				return new OrcSplitReader.Not(
						new OrcSplitReader.LessThan(tuple3.f0, tuple3.f1, tuple3.f2));
			} else {
				return new OrcSplitReader.LessThanEquals(tuple3.f0, tuple3.f1, tuple3.f2);
			}
		};
	}

	private static Function<CallExpression, OrcSplitReader.Predicate> createLessThanPredicateConverter(){
		return callExp -> {

			Tuple3<String, PredicateLeaf.Type, Serializable> tuple3 = getTuple3Args(callExp);

			if (tuple3 == null){
				return null;
			}

			boolean literalOnRight = literalOnRight(callExp);

			if (literalOnRight) {
				return new OrcSplitReader.LessThan(tuple3.f0, tuple3.f1, tuple3.f2);
			} else {
				return new OrcSplitReader.Not(
						new OrcSplitReader.LessThanEquals(tuple3.f0, tuple3.f1, tuple3.f2));
			}
		};
	}

	private static Function<CallExpression, OrcSplitReader.Predicate> createLessThanEqualsPredicateConverter(){
		return callExp -> {

			Tuple3<String, PredicateLeaf.Type, Serializable> tuple3 = getTuple3Args(callExp);

			if (tuple3 == null){
				return null;
			}

			boolean literalOnRight = literalOnRight(callExp);

			if (literalOnRight) {
				return new OrcSplitReader.LessThanEquals(tuple3.f0, tuple3.f1, tuple3.f2);
			} else {
				return new OrcSplitReader.Not(
						new OrcSplitReader.LessThan(tuple3.f0, tuple3.f1, tuple3.f2));
			}
		};
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
		} else if (comp.getChildren().get(0) instanceof ValueLiteralExpression
				&& comp.getChildren().get(1) instanceof FieldReferenceExpression) {
			return false;
		} else if (comp.getChildren().get(0) instanceof FieldReferenceExpression
				&& comp.getChildren().get(1) instanceof ValueLiteralExpression) {
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
		switch (ltype){
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
			case VARCHAR:
				return PredicateLeaf.Type.STRING;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
			case TIMESTAMP_WITH_TIME_ZONE:
				return PredicateLeaf.Type.TIMESTAMP;
			case DATE:
				return PredicateLeaf.Type.DATE;
			case BINARY:
				return PredicateLeaf.Type.DECIMAL;
			default:
				return null;
		}
	}
}
