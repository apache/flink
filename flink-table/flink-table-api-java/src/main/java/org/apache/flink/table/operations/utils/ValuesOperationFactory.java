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
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsExplicitCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

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
				DataType targetDataType = dataTypes[i];

				return convertToExpectedType(castedExpr, targetDataType, postResolverFactory)
					.orElseThrow(() -> new ValidationException(String.format(
						"Could not cast the value of the %d column: [ %s ] of a row: %s to the requested type: %s",
						i,
						castedExpr.asSummaryString(),
						row.stream()
							.map(ResolvedExpression::asSummaryString)
							.collect(Collectors.joining(", ", "[ ", " ]")),
						targetDataType.getLogicalType().asSummaryString())));
			})
			.collect(Collectors.toList());
	}

	private Optional<ResolvedExpression> convertToExpectedType(
			ResolvedExpression sourceExpression,
			DataType targetDataType,
			ExpressionResolver.PostResolverFactory postResolverFactory) {

		LogicalType sourceLogicalType = sourceExpression.getOutputDataType().getLogicalType();
		LogicalType targetLogicalType = targetDataType.getLogicalType();

		// if the expression is a literal try converting the literal in place instead of casting
		if (sourceExpression instanceof ValueLiteralExpression) {
			// Assign a type to a null literal
			if (hasRoot(sourceLogicalType, LogicalTypeRoot.NULL)) {
				return Optional.of(valueLiteral(null, targetDataType));
			}

			// Check if the source value class is a valid input conversion class of the target type
			// It may happen that a user wanted to use a secondary input conversion class as a value for
			// a different type than what we derived.
			//
			// Example: we interpreted 1L as BIGINT, but user wanted to interpret it as a TIMESTAMP
			// In this case long is a valid conversion class for TIMESTAMP, but a
			// cast from BIGINT to TIMESTAMP is an invalid operation.
			Optional<Object> value = ((ValueLiteralExpression) sourceExpression).getValueAs(Object.class);
			if (value.isPresent() && targetLogicalType.supportsInputConversion(value.get().getClass())) {
				ValueLiteralExpression convertedLiteral = valueLiteral(
					value.get(),
					targetDataType.notNull().bridgedTo(value.get().getClass()));
				if (targetLogicalType.isNullable()) {
					return Optional.of(postResolverFactory.cast(convertedLiteral, targetDataType));
				} else {
					return Optional.of(convertedLiteral);
				}
			}
		}

		if (sourceExpression instanceof CallExpression) {
			FunctionDefinition functionDefinition = ((CallExpression) sourceExpression).getFunctionDefinition();
			if (functionDefinition == BuiltInFunctionDefinitions.ROW &&
					hasRoot(targetLogicalType, LogicalTypeRoot.ROW)) {
				return convertRowToExpectedType(sourceExpression, (FieldsDataType) targetDataType, postResolverFactory);
			} else if (functionDefinition == BuiltInFunctionDefinitions.ARRAY &&
						hasRoot(targetLogicalType, LogicalTypeRoot.ARRAY)) {
				return convertArrayToExpectedType(
					sourceExpression,
					(CollectionDataType) targetDataType,
					postResolverFactory);
			} else if (functionDefinition == BuiltInFunctionDefinitions.MAP &&
						hasRoot(targetLogicalType, LogicalTypeRoot.MAP)) {
				return convertMapToExpectedType(
					sourceExpression,
					(KeyValueDataType) targetDataType,
					postResolverFactory);
			}
		}

		// We might not be able to cast to the expected type if the expected type was provided by the user
		// we ignore nullability constraints here, as we let users override what we expect there, e.g. they
		// might know that a certain function will not produce nullable values for a given input
		if (supportsExplicitCast(
				sourceLogicalType.copy(true),
				targetLogicalType.copy(true))) {
			return Optional.of(postResolverFactory.cast(sourceExpression, targetDataType));
		} else {
			return Optional.empty();
		}
	}

	private Optional<ResolvedExpression> convertRowToExpectedType(
			ResolvedExpression sourceExpression,
			FieldsDataType targetDataType,
			ExpressionResolver.PostResolverFactory postResolverFactory) {
		List<DataType> targetDataTypes = targetDataType.getChildren();
		List<ResolvedExpression> resolvedChildren = sourceExpression.getResolvedChildren();

		if (resolvedChildren.size() != targetDataTypes.size()) {
			return Optional.empty();
		}

		ResolvedExpression[] castedChildren = new ResolvedExpression[resolvedChildren.size()];
		for (int i = 0; i < resolvedChildren.size(); i++) {
			boolean typesMatch = resolvedChildren.get(i)
				.getOutputDataType()
				.getLogicalType()
				.equals(targetDataTypes.get(i).getLogicalType());
			if (typesMatch) {
				castedChildren[i] = resolvedChildren.get(i);
			}

			ResolvedExpression child = resolvedChildren.get(i);
			DataType targetChildDataType = targetDataTypes.get(i);

			Optional<ResolvedExpression> castedChild = convertToExpectedType(
				child,
				targetChildDataType,
				postResolverFactory);

			if (!castedChild.isPresent()) {
				return Optional.empty();
			} else {
				castedChildren[i] = castedChild.get();
			}
		}

		return Optional.of(postResolverFactory.row(targetDataType, castedChildren));
	}

	private Optional<ResolvedExpression> convertArrayToExpectedType(
			ResolvedExpression sourceExpression,
			CollectionDataType targetDataType,
			ExpressionResolver.PostResolverFactory postResolverFactory) {
		DataType elementTargetDataType = targetDataType.getElementDataType();
		List<ResolvedExpression> resolvedChildren = sourceExpression.getResolvedChildren();
		ResolvedExpression[] castedChildren = new ResolvedExpression[resolvedChildren.size()];
		for (int i = 0; i < resolvedChildren.size(); i++) {
			Optional<ResolvedExpression> castedChild = convertToExpectedType(
				resolvedChildren.get(i),
				elementTargetDataType,
				postResolverFactory);
			if (castedChild.isPresent()) {
				castedChildren[i] = castedChild.get();
			} else {
				return Optional.empty();
			}
		}
		return Optional.of(postResolverFactory.array(targetDataType, castedChildren));
	}

	private Optional<ResolvedExpression> convertMapToExpectedType(
			ResolvedExpression sourceExpression,
			KeyValueDataType targetDataType,
			ExpressionResolver.PostResolverFactory postResolverFactory) {
		DataType keyTargetDataType = targetDataType.getKeyDataType();
		DataType valueTargetDataType = targetDataType.getValueDataType();
		List<ResolvedExpression> resolvedChildren = sourceExpression.getResolvedChildren();
		ResolvedExpression[] castedChildren = new ResolvedExpression[resolvedChildren.size()];
		for (int i = 0; i < resolvedChildren.size(); i++) {
			Optional<ResolvedExpression> castedChild = convertToExpectedType(
				resolvedChildren.get(i),
				i % 2 == 0 ? keyTargetDataType : valueTargetDataType,
				postResolverFactory);
			if (castedChild.isPresent()) {
				castedChildren[i] = castedChild.get();
			} else {
				return Optional.empty();
			}
		}

		return Optional.of(postResolverFactory.map(targetDataType, castedChildren));
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
			LogicalType outputLogicalType = resolvedExpression.get(rowPosition).getOutputDataType().getLogicalType();
			typesAtIPosition.add(outputLogicalType);
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
