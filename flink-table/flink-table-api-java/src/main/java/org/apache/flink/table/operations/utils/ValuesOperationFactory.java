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

package org.apache.flink.table.operations.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ValuesQueryOperation;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeGeneralization;
import org.apache.flink.table.types.utils.TypeConversions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;

/**
 * Utility class for creating valid {@link ValuesQueryOperation} operation.
 */
@Internal
class ValuesOperationFactory {
	/**
	 * Creates a valid {@link ValuesQueryOperation} operation.
	 *
	 * <p>It derives a row type based on {@link LogicalTypeGeneralization}. It flattens any
	 * row constructors. It does not flatten ROWs which are a result of e.g. a function call.
	 *
	 * <p>The resulting schema can be provided manually. If it is not, the schema will be automatically derived from
	 * the types of the expressions.
	 */
	QueryOperation create(
			@Nullable TableSchema expectedSchema,
			List<ResolvedExpression> resolvedExpressions,
			ExpressionResolver.PostResolverFactory postResolverFactory) {
		List<List<ResolvedExpression>> resolvedRows = unwrapFromRowConstructor(resolvedExpressions);

		if (expectedSchema != null) {
			verifyAllSameSize(resolvedRows, expectedSchema.getFieldCount());
		}

		TableSchema schema = Optional.ofNullable(expectedSchema)
			.orElseGet(() -> extractSchema(resolvedRows));

		List<List<ResolvedExpression>> castedExpressions = resolvedRows.stream()
			.map(row -> convertTopLevelExpressionToExpectedRowType(postResolverFactory, schema.getFieldDataTypes(), row))
			.collect(Collectors.toList());

		return new ValuesQueryOperation(castedExpressions, schema);
	}

	private TableSchema extractSchema(List<List<ResolvedExpression>> resolvedRows) {
		DataType[] dataTypes = findRowType(resolvedRows);
		String[] fieldNames = IntStream.range(0, dataTypes.length)
			.mapToObj(i -> "f" + i)
			.toArray(String[]::new);
		return TableSchema.builder()
			.fields(fieldNames, dataTypes)
			.build();
	}

	private List<ResolvedExpression> convertTopLevelExpressionToExpectedRowType(
			ExpressionResolver.PostResolverFactory postResolverFactory,
			DataType[] dataTypes,
			List<ResolvedExpression> row) {
		return IntStream.range(0, row.size())
			.mapToObj(i -> {
				boolean typesMatch = row.get(i)
					.getOutputDataType()
					.getLogicalType()
					.equals(dataTypes[i].getLogicalType());
				if (typesMatch) {
					return row.get(i);
				}

				ResolvedExpression castedExpr = row.get(i);
				DataType targetType = dataTypes[i];

				return convertToExpectedType(castedExpr, targetType, postResolverFactory)
					.orElseThrow(() -> new ValidationException(String.format(
						"Could not cast the value of the %d column: [ %s ] of a row: %s to the requested type: %s",
						i,
						castedExpr.asSummaryString(),
						row.stream()
							.map(ResolvedExpression::asSummaryString)
							.collect(Collectors.joining(", ", "[ ", " ]")),
						targetType.getLogicalType().asSummaryString())));
			})
			.collect(Collectors.toList());
	}

	private Optional<ResolvedExpression> convertToExpectedType(
			ResolvedExpression castedExpr,
			DataType targetType,
			ExpressionResolver.PostResolverFactory postResolverFactory) {

		// if the expression is a literal try converting the literal in place instead of casting
		if (castedExpr instanceof ValueLiteralExpression) {
			// Assign a type to a null literal
			if (LogicalTypeChecks.hasRoot(castedExpr.getOutputDataType().getLogicalType(), LogicalTypeRoot.NULL)) {
				return Optional.of(valueLiteral(null, targetType));
			}

			Optional<Object> value = ((ValueLiteralExpression) castedExpr).getValueAs(Object.class);
			if (value.isPresent() && targetType.getLogicalType().supportsInputConversion(value.get().getClass())) {
				ValueLiteralExpression convertedLiteral = valueLiteral(
					value.get(),
					targetType.notNull().bridgedTo(value.get().getClass()));
				if (targetType.getLogicalType().isNullable()) {
					return Optional.of(postResolverFactory.cast(convertedLiteral, targetType));
				} else {
					return Optional.of(convertedLiteral);
				}
			}
		}

		if (castedExpr instanceof CallExpression) {
			FunctionDefinition functionDefinition = ((CallExpression) castedExpr).getFunctionDefinition();
			if (functionDefinition == BuiltInFunctionDefinitions.ROW && targetType instanceof FieldsDataType) {
				return convertRowToExpectedType(castedExpr, (FieldsDataType) targetType, postResolverFactory);
			} else if (functionDefinition == BuiltInFunctionDefinitions.ARRAY &&
						LogicalTypeChecks.hasRoot(targetType.getLogicalType(), LogicalTypeRoot.ARRAY)) {
				return convertArrayToExpectedType(castedExpr, (CollectionDataType) targetType, postResolverFactory);
			} else if (functionDefinition == BuiltInFunctionDefinitions.MAP &&
						LogicalTypeChecks.hasRoot(targetType.getLogicalType(), LogicalTypeRoot.MAP)) {
				return convertMapToExpectedType(castedExpr, (KeyValueDataType) targetType, postResolverFactory);
			}
		}

		// We might not be able to cast to the expected type if the expected type was provided by the user
		// we ignore nullability constraints here, as we let users override what we expect there, e.g. they
		// might know that a certain function will not produce nullable values for a given input
		if (LogicalTypeCasts.supportsExplicitCast(
				castedExpr.getOutputDataType().getLogicalType().copy(true),
				targetType.getLogicalType().copy(true))) {
			return Optional.of(postResolverFactory.cast(castedExpr, targetType));
		} else {
			return Optional.empty();
		}
	}

	private Optional<ResolvedExpression> convertRowToExpectedType(
			ResolvedExpression castedExpr,
			FieldsDataType targetType,
			ExpressionResolver.PostResolverFactory postResolverFactory) {
		DataType[] targetTypes = ((RowType) targetType.getLogicalType()).getFieldNames()
			.stream()
			.map(name -> targetType.getFieldDataTypes().get(name))
			.toArray(DataType[]::new);
		List<ResolvedExpression> resolvedChildren = castedExpr.getResolvedChildren();

		if (resolvedChildren.size() != targetTypes.length) {
			return Optional.empty();
		}

		ResolvedExpression[] castedChildren = new ResolvedExpression[resolvedChildren.size()];
		for (int i = 0; i < resolvedChildren.size(); i++) {
			boolean typesMatch = resolvedChildren.get(i)
				.getOutputDataType()
				.getLogicalType()
				.equals(targetTypes[i].getLogicalType());
			if (typesMatch) {
				castedChildren[i] = resolvedChildren.get(i);
			}

			ResolvedExpression child = resolvedChildren.get(i);
			DataType targetChildType = targetTypes[i];

			Optional<ResolvedExpression> castedChild = convertToExpectedType(
				child,
				targetChildType,
				postResolverFactory);

			if (!castedChild.isPresent()) {
				return Optional.empty();
			} else {
				castedChildren[i] = castedChild.get();
			}
		}

		return Optional.of(postResolverFactory.row(targetType, castedChildren));
	}

	private Optional<ResolvedExpression> convertArrayToExpectedType(
			ResolvedExpression castedExpr,
			CollectionDataType targetType,
			ExpressionResolver.PostResolverFactory postResolverFactory) {
		DataType elementTargetType = targetType.getElementDataType();
		List<ResolvedExpression> resolvedChildren = castedExpr.getResolvedChildren();
		ResolvedExpression[] castedChildren = new ResolvedExpression[resolvedChildren.size()];
		for (int i = 0; i < resolvedChildren.size(); i++) {
			Optional<ResolvedExpression> castedChild = convertToExpectedType(
				resolvedChildren.get(i),
				elementTargetType,
				postResolverFactory);
			if (castedChild.isPresent()) {
				castedChildren[i] = castedChild.get();
			} else {
				return Optional.empty();
			}
		}
		return Optional.of(postResolverFactory.array(targetType, castedChildren));
	}

	private Optional<ResolvedExpression> convertMapToExpectedType(
			ResolvedExpression castedExpr,
			KeyValueDataType targetType,
			ExpressionResolver.PostResolverFactory postResolverFactory) {
		DataType keyTargetType = targetType.getKeyDataType();
		DataType valueTargetType = targetType.getValueDataType();
		List<ResolvedExpression> resolvedChildren = castedExpr.getResolvedChildren();
		ResolvedExpression[] castedChildren = new ResolvedExpression[resolvedChildren.size()];
		for (int i = 0; i < resolvedChildren.size(); i++) {
			Optional<ResolvedExpression> castedChild = convertToExpectedType(
				resolvedChildren.get(i),
				i % 2 == 0 ? keyTargetType : valueTargetType,
				postResolverFactory);
			if (castedChild.isPresent()) {
				castedChildren[i] = castedChild.get();
			} else {
				return Optional.empty();
			}
		}

		return Optional.of(postResolverFactory.map(targetType, castedChildren));
	}

	private List<List<ResolvedExpression>> unwrapFromRowConstructor(List<ResolvedExpression> resolvedExpressions) {
		return resolvedExpressions
			.stream()
			.map(expr -> expr.accept(
				new ExpressionDefaultVisitor<List<ResolvedExpression>>() {
					@Override
					public List<ResolvedExpression> visit(CallExpression call) {
						if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.ROW) {
							return call.getResolvedChildren();
						}

						return defaultMethod(call);
					}

					@Override
					protected List<ResolvedExpression> defaultMethod(Expression expression) {
						if (!(expression instanceof ResolvedExpression)) {
							throw new TableException(
								"This visitor is applied to ResolvedExpressions. We should never end up here.");
						}

						return Collections.singletonList((ResolvedExpression) expression);
					}
				}))
			.collect(Collectors.toList());
	}

	private DataType[] findRowType(List<List<ResolvedExpression>> resolvedRows) {
		int rowSize = findRowSize(resolvedRows);
		DataType[] dataTypes = new DataType[rowSize];
		IntStream.range(0, rowSize).forEach(i -> {
			dataTypes[i] = findCommonTypeAtPosition(resolvedRows, i);
		});
		return dataTypes;
	}

	private DataType findCommonTypeAtPosition(List<List<ResolvedExpression>> resolvedRows, int i) {
		List<LogicalType> typesAtIPosition = extractLogicalTypesAtPosition(resolvedRows, i);

		LogicalType logicalType = LogicalTypeGeneralization.findCommonType(typesAtIPosition)
			.orElseThrow(() -> {
				Set<DataType> columnTypes = resolvedRows.stream()
					.map(row -> row.get(i).getOutputDataType())
					.collect(Collectors.toCollection(LinkedHashSet::new));

				return new ValidationException(String.format(
					"Types in fromValues(...) must have a common super type. Could not find a common type" +
						" for all rows at column %d.\n" +
						"Could not find a common super type for types: %s",
					i,
					columnTypes));
			});

		return TypeConversions.fromLogicalToDataType(logicalType);
	}

	private List<LogicalType> extractLogicalTypesAtPosition(
			List<List<ResolvedExpression>> resolvedRows,
			int rowPosition) {
		List<LogicalType> typesAtIPosition = new ArrayList<>();
		for (List<ResolvedExpression> resolvedExpression : resolvedRows) {
			LogicalType outputDataType = resolvedExpression.get(rowPosition).getOutputDataType().getLogicalType();
			typesAtIPosition.add(outputDataType);
		}
		return typesAtIPosition;
	}

	private int findRowSize(List<List<ResolvedExpression>> resolvedRows) {
		List<ResolvedExpression> firstRow = resolvedRows.get(0);
		int potentialRowSize = firstRow.size();
		verifyAllSameSize(resolvedRows, potentialRowSize);
		return potentialRowSize;
	}

	private void verifyAllSameSize(List<List<ResolvedExpression>> resolvedRows, int potentialRowSize) {
		Optional<List<ResolvedExpression>> differentSizeRow = resolvedRows.stream()
			.filter(row -> row.size() != potentialRowSize)
			.findAny();
		if (differentSizeRow.isPresent()) {
			throw new ValidationException(String.format(
				"All rows in a fromValues(...) clause must have the same fields number. Row %s has a" +
					" different length than the expected size: %d.",
				differentSizeRow.get(),
				potentialRowSize));
		}
	}
}
