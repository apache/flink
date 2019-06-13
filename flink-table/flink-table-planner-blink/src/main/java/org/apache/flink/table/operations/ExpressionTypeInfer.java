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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.AggregateFunctionDefinition;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.InternalFunctionDefinitions;
import org.apache.flink.table.expressions.ResolvedAggInputReference;
import org.apache.flink.table.expressions.ResolvedAggLocalReference;
import org.apache.flink.table.expressions.ResolvedDistinctKeyReference;
import org.apache.flink.table.expressions.ScalarFunctionDefinition;
import org.apache.flink.table.expressions.TableFunctionDefinition;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.typeutils.TypeCoercion;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.expressions.ExpressionUtils.extractValue;
import static org.apache.flink.table.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;
import static org.apache.flink.table.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * {@link DataType} infer for {@link Expression}.
 */
public class ExpressionTypeInfer implements ExpressionVisitor<DataType> {

	private static final ExpressionTypeInfer INSTANCE = new ExpressionTypeInfer();
	private static final Map<FunctionDefinition, CallTypeInference> CALL_TYPE_INFERENCES = new HashMap<>();
	private static final CallTypeInference STRING_INFER = call -> DataTypes.STRING();
	private static final CallTypeInference BOOLEAN_INFER = call -> DataTypes.BOOLEAN();
	private static final CallTypeInference DOUBLE_INFER = call -> DataTypes.DOUBLE();
	private static final CallTypeInference INT_INFER = call -> DataTypes.INT();
	private static final CallTypeInference BIGINT_INFER = call -> DataTypes.BIGINT();
	private static final CallTypeInference DATE_INFER = call -> DataTypes.DATE();
	private static final CallTypeInference TIME_INFER = call -> DataTypes.TIME();
	private static final CallTypeInference TIMESTAMP_INFER = call -> DataTypes.TIMESTAMP(3);
	private static final CallTypeInference BINARY_ARITHMETIC_INFER = call -> {
		scala.Option<LogicalType> returnType = TypeCoercion.widerTypeOf(
				fromDataTypeToLogicalType(infer(call.getChildren().get(0))),
				fromDataTypeToLogicalType(infer(call.getChildren().get(1))));
		if (returnType.isEmpty()) {
			throw new ValidationException("TypeCoercion fail.");
		} else {
			return fromLogicalTypeToDataType(returnType.get());
		}
	};
	private static final CallTypeInference FIRST_CHILD_INFER = call -> infer(call.getChildren().get(0));
	private static final CallTypeInference SECOND_CHILD_INFER = call -> infer(call.getChildren().get(1));
	static {
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.AND, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.OR, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.NOT, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.IF, SECOND_CHILD_INFER);

		// comparison functions
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.EQUALS, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES
				.put(BuiltInFunctionDefinitions.GREATER_THAN, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES
				.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.LESS_THAN, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES
				.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.NOT_EQUALS, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.IS_NULL, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.IS_NOT_NULL, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.IS_TRUE, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.IS_FALSE, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.IS_NOT_TRUE, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.IS_NOT_FALSE, BOOLEAN_INFER);

		// string functions
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.CHAR_LENGTH, INT_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.INIT_CAP, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.LIKE, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.LOWER, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SIMILAR, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SUBSTRING, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.UPPER, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.POSITION, INT_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.OVERLAY, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.CONCAT, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.CONCAT_WS, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.LPAD, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.RPAD, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.REGEXP_EXTRACT, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.FROM_BASE64, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.TO_BASE64, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.UUID, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.LTRIM, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.RTRIM, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.REPEAT, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.REGEXP_REPLACE, STRING_INFER);

		// math functions
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.PLUS, BINARY_ARITHMETIC_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.MOD, BINARY_ARITHMETIC_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.MINUS, BINARY_ARITHMETIC_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.DIVIDE, BINARY_ARITHMETIC_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.TIMES, BINARY_ARITHMETIC_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ABS, FIRST_CHILD_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.EXP, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.LOG10, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.LOG2, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.LN, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.LOG, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.POWER, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SQRT, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.MINUS_PREFIX, FIRST_CHILD_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SIN, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.COS, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SINH, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.TAN, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.TANH, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.COT, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ASIN, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ACOS, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ATAN, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ATAN2, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.COSH, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.DEGREES, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.RADIANS, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SIGN, FIRST_CHILD_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ROUND, FIRST_CHILD_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.PI, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.E, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.RAND, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.RAND_INTEGER, INT_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.BIN, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.HEX, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.TRUNCATE, FIRST_CHILD_INFER);

		// time functions
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.EXTRACT, BIGINT_INFER);
		CALL_TYPE_INFERENCES
				.put(BuiltInFunctionDefinitions.CURRENT_DATE, DATE_INFER);
		CALL_TYPE_INFERENCES
				.put(BuiltInFunctionDefinitions.CURRENT_TIME, TIME_INFER);
		CALL_TYPE_INFERENCES
				.put(BuiltInFunctionDefinitions.CURRENT_TIMESTAMP, TIMESTAMP_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.LOCAL_TIME, TIME_INFER);
		CALL_TYPE_INFERENCES
				.put(BuiltInFunctionDefinitions.LOCAL_TIMESTAMP, TIMESTAMP_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.DATE_FORMAT, STRING_INFER);

		// collection
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.AT, DOUBLE_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.CARDINALITY, INT_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ORDER_DESC, FIRST_CHILD_INFER);

		// crypto hash
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.MD5, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SHA2, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SHA224, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SHA256, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SHA384, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SHA512, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.SHA1, STRING_INFER);

		CALL_TYPE_INFERENCES.put(InternalFunctionDefinitions.THROW_EXCEPTION, FIRST_CHILD_INFER);

		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.CAST,
				call -> ((TypeLiteralExpression) call.getChildren().get(1)).getOutputDataType());
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.REINTERPRET_CAST,
				call -> ((TypeLiteralExpression) call.getChildren().get(1)).getOutputDataType());
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.IN, BOOLEAN_INFER);

		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.GET,
				call -> {
					ValueLiteralExpression keyLiteral = (ValueLiteralExpression) call.getChildren().get(1);
					DataType child = infer(call.getChildren().get(0));

					String[] fieldNames;
					Function<String, DataType> nameToType;
					if (child instanceof FieldsDataType) {
						FieldsDataType compositeType = (FieldsDataType) child;
						fieldNames = ((RowType) compositeType.getLogicalType()).getFieldNames().toArray(new String[0]);
						nameToType = compositeType.getFieldDataTypes()::get;
					} else {
						CompositeType compositeType = (CompositeType) fromDataTypeToLegacyInfo(child);
						fieldNames = compositeType.getFieldNames();
						nameToType = s -> fromLegacyInfoToDataType(compositeType.getTypeAt(Arrays.asList(fieldNames).indexOf(s)));
					}

					DataType ret = nameToType.apply(extractValue(keyLiteral, String.class).orElseGet(() -> {
						Optional<Integer> pos = extractValue(keyLiteral, Integer.class);
						if (pos.isPresent()) {
							return fieldNames[pos.get()];
						} else {
							throw new RuntimeException("Unknown keyLiteral: " + keyLiteral);
						}
					}));
					if (ret == null) {
						throw new ValidationException("Get type fail.");
					}
					return ret;
				});
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.TRIM, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.AS, FIRST_CHILD_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.OVER, FIRST_CHILD_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ORDER_ASC, FIRST_CHILD_INFER);

		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.BETWEEN, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.NOT_BETWEEN, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.REPLACE, STRING_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.CEIL, BIGINT_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.FLOOR, BIGINT_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS, BOOLEAN_INFER);
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.TIMESTAMP_DIFF, INT_INFER);

		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ARRAY_ELEMENT,
				call -> {
					DataType child = infer(call.getChildren().get(0));
					if (child instanceof CollectionDataType) {
						CollectionDataType collectionType = (CollectionDataType) child;
						return collectionType.getElementDataType();
					} else {
						return fromLegacyInfoToDataType(((BasicArrayTypeInfo) fromDataTypeToLegacyInfo(child)).getComponentInfo());
					}
				});
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ARRAY, call -> DataTypes.ARRAY(infer(call.getChildren().get(0))));
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.MAP,
				call -> {
					DataType head = infer(call.getChildren().get(0));
					DataType last = infer(call.getChildren().get(call.getChildren().size() - 1));
					return DataTypes.MAP(head, last);
				});
		CALL_TYPE_INFERENCES.put(BuiltInFunctionDefinitions.ROW,
				call -> {
					DataTypes.Field[] fields = new DataTypes.Field[call.getChildren().size()];
					for (int i = 0; i < call.getChildren().size(); i++) {
						fields[i] = DataTypes.FIELD("f" + i, infer(call.getChildren().get(i)));
					}
					return DataTypes.ROW(fields);
				});
	}

	private ExpressionTypeInfer() {}

	public static DataType infer(Expression expression) {
		return expression.accept(INSTANCE);
	}

	@Override
	public DataType visitCall(CallExpression call) {
		FunctionDefinition def = call.getFunctionDefinition();
		FunctionDefinition definition = call.getFunctionDefinition();
		if (definition instanceof ScalarFunctionDefinition) {
			ScalarFunction scalaFunc = ((ScalarFunctionDefinition) definition).getScalarFunction();
			return UserDefinedFunctionUtils.getResultTypeOfScalarFunction(
					scalaFunc,
					new Object[call.getChildren().size()],
					call.getChildren().stream().map(ExpressionTypeInfer::infer)
							.map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
							.toArray(LogicalType[]::new));
		} else if (definition instanceof TableFunctionDefinition) {
			return fromLegacyInfoToDataType(((TableFunctionDefinition) definition).getResultType());
		} else if (definition instanceof AggregateFunctionDefinition) {
			return fromLegacyInfoToDataType(((AggregateFunctionDefinition) definition).getResultTypeInfo());
		} else {
			CallTypeInference infer = CALL_TYPE_INFERENCES.get(definition);
			if (infer != null) {
				return infer.infer(call);
			} else {
				throw new RuntimeException("Not support yet: " + def);
			}
		}
	}

	@Override
	public DataType visitValueLiteral(ValueLiteralExpression valueLiteral) {
		return valueLiteral.getOutputDataType();
	}

	@Override
	public DataType visitFieldReference(FieldReferenceExpression fieldReference) {
		return fieldReference.getOutputDataType();
	}

	@Override
	public DataType visitTypeLiteral(TypeLiteralExpression typeLiteral) {
		return typeLiteral.getOutputDataType();
	}

	@Override
	public DataType visit(Expression other) {
		if (other instanceof ResolvedAggInputReference) {
			return fromLogicalToDataType(((ResolvedAggInputReference) other).getResultType());
		} else if (other instanceof ResolvedAggLocalReference) {
			return fromLogicalToDataType(((ResolvedAggLocalReference) other).getResultType());
		} else if (other instanceof ResolvedDistinctKeyReference) {
			return fromLogicalToDataType(((ResolvedDistinctKeyReference) other).getResultType());
		} else {
			throw new UnsupportedOperationException(other.getClass().getSimpleName() + ":" + other.toString());
		}
	}

	/**
	 * Type infer for {@link CallExpression}.
	 */
	private interface CallTypeInference {
		DataType infer(CallExpression call);
	}
}
