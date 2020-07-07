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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.SessionWithGapOnTimeWithAlias;
import org.apache.flink.table.api.SlideWithSizeAndSlideOnTimeWithAlias;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TumbleWithSizeOnTimeWithAlias;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.expressions.utils.ResolvedExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.operations.AggregateQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.FieldInfoUtils;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
import static org.apache.flink.table.functions.FunctionKind.TABLE_AGGREGATE;
import static org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow.WindowType.SLIDE;
import static org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow.WindowType.TUMBLE;
import static org.apache.flink.table.operations.utils.OperationExpressionsUtils.extractName;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isTimeAttribute;

/**
 * Utility class for creating a valid {@link AggregateQueryOperation} or {@link WindowAggregateQueryOperation}.
 */
@Internal
final class AggregateOperationFactory {

	private final boolean isStreamingMode;
	private final NoNestedAggregates noNestedAggregates = new NoNestedAggregates();
	private final ValidateDistinct validateDistinct = new ValidateDistinct();
	private final AggregationExpressionValidator aggregationsValidator = new AggregationExpressionValidator();
	private final IsKeyTypeChecker isKeyTypeChecker = new IsKeyTypeChecker();

	AggregateOperationFactory(boolean isStreamingMode) {
		this.isStreamingMode = isStreamingMode;
	}

	/**
	 * Creates a valid {@link AggregateQueryOperation} operation.
	 *
	 * @param groupings expressions describing grouping key of aggregates
	 * @param aggregates expressions describing aggregation functions
	 * @param child relational operation on top of which to apply the aggregation
	 * @return valid aggregate operation
	 */
	QueryOperation createAggregate(
			List<ResolvedExpression> groupings,
			List<ResolvedExpression> aggregates,
			QueryOperation child) {
		validateGroupings(groupings);
		validateAggregates(aggregates);

		DataType[] fieldTypes = Stream.concat(
			groupings.stream().map(ResolvedExpression::getOutputDataType),
			aggregates.stream().flatMap(this::extractAggregateResultTypes)
		).toArray(DataType[]::new);

		String[] groupNames = groupings.stream()
			.map(expr -> extractName(expr).orElseGet(expr::toString)).toArray(String[]::new);
		String[] fieldNames = Stream.concat(
			Stream.of(groupNames),
			aggregates.stream().flatMap(p -> extractAggregateNames(p, Arrays.asList(groupNames)))
		).toArray(String[]::new);

		TableSchema tableSchema = TableSchema.builder().fields(fieldNames, fieldTypes).build();

		return new AggregateQueryOperation(groupings, aggregates, child, tableSchema);
	}

	/**
	 * Creates a valid {@link WindowAggregateQueryOperation} operation.
	 *
	 * @param groupings expressions describing grouping key of aggregates
	 * @param aggregates expressions describing aggregation functions
	 * @param windowProperties expressions describing window properties
	 * @param window grouping window of this aggregation
	 * @param child relational operation on top of which to apply the aggregation
	 * @return valid window aggregate operation
	 */
	QueryOperation createWindowAggregate(
			List<ResolvedExpression> groupings,
			List<ResolvedExpression> aggregates,
			List<ResolvedExpression> windowProperties,
			ResolvedGroupWindow window,
			QueryOperation child) {
		validateGroupings(groupings);
		validateAggregates(aggregates);
		validateWindowProperties(windowProperties, window);

		DataType[] fieldTypes = concat(
			groupings.stream().map(ResolvedExpression::getOutputDataType),
			aggregates.stream().flatMap(this::extractAggregateResultTypes),
			windowProperties.stream().map(ResolvedExpression::getOutputDataType)
		).toArray(DataType[]::new);

		String[] groupNames = groupings.stream()
			.map(expr -> extractName(expr).orElseGet(expr::toString)).toArray(String[]::new);
		String[] fieldNames = concat(
			Stream.of(groupNames),
			aggregates.stream().flatMap(p -> extractAggregateNames(p, Arrays.asList(groupNames))),
			windowProperties.stream().map(expr -> extractName(expr).orElseGet(expr::toString))
		).toArray(String[]::new);

		TableSchema tableSchema = TableSchema.builder().fields(fieldNames, fieldTypes).build();

		return new WindowAggregateQueryOperation(
			groupings,
			aggregates,
			windowProperties,
			window,
			child,
			tableSchema);
	}

	/**
	 * Extract result types for the aggregate or the table aggregate expression. For a table aggregate,
	 * it may return multi result types when the composite return type is flattened.
	 */
	private Stream<DataType> extractAggregateResultTypes(ResolvedExpression expression) {
		if (ApiExpressionUtils.isFunctionOfKind(expression, TABLE_AGGREGATE)) {
			TypeInformation<?> legacyInfo = TypeConversions.fromDataTypeToLegacyInfo(expression.getOutputDataType());
			return Stream.of(FieldInfoUtils.getFieldTypes(legacyInfo))
				.map(TypeConversions::fromLegacyInfoToDataType);
		} else {
			return Stream.of(expression.getOutputDataType());
		}
	}

	/**
	 * Extract names for the aggregate or the table aggregate expression. For a table aggregate, it
	 * may return multi output names when the composite return type is flattened. If the result type
	 * is not a {@link CompositeType}, the result name should not conflict with the group names.
	 */
	private Stream<String> extractAggregateNames(Expression expression, List<String> groupNames) {
		if (isFunctionOfKind(expression, TABLE_AGGREGATE)) {
			final TableAggregateFunctionDefinition definition =
				(TableAggregateFunctionDefinition) ((CallExpression) expression).getFunctionDefinition();
			return Arrays.stream(FieldInfoUtils.getFieldNames(definition.getResultTypeInfo(), groupNames));
		} else {
			return Stream.of(extractName(expression).orElseGet(expression::toString));
		}
	}

	/**
	 * Converts an API class to a resolved window for planning with expressions already resolved.
	 * It performs following validations:
	 * <ul>
	 *     <li>The alias is represented with an unresolved reference</li>
	 *     <li>The time attribute is a single field reference of a {@link TimeIndicatorTypeInfo}(stream),
	 *     {@link SqlTimeTypeInfo}(batch), or {@link BasicTypeInfo#LONG_TYPE_INFO}(batch) type</li>
	 *     <li>The size & slide are value literals of either {@link BasicTypeInfo#LONG_TYPE_INFO},
	 *     or {@link TimeIntervalTypeInfo} type</li>
	 *     <li>The size & slide are of the same type</li>
	 *     <li>The gap is a value literal of a {@link TimeIntervalTypeInfo} type</li>
	 * </ul>
	 *
	 * @param window window to resolve
	 * @param resolver resolver to resolve potential unresolved field references
	 * @return window with expressions resolved
	 */
	ResolvedGroupWindow createResolvedWindow(GroupWindow window, ExpressionResolver resolver) {
		Expression alias = window.getAlias();

		if (!(alias instanceof UnresolvedReferenceExpression)) {
			throw new ValidationException("Only unresolved reference supported for alias of a group window.");
		}

		final String windowName = ((UnresolvedReferenceExpression) alias).getName();
		FieldReferenceExpression timeField = getValidatedTimeAttribute(window, resolver);

		if (window instanceof TumbleWithSizeOnTimeWithAlias) {
			return validateAndCreateTumbleWindow(
				(TumbleWithSizeOnTimeWithAlias) window,
				windowName,
				timeField);
		} else if (window instanceof SlideWithSizeAndSlideOnTimeWithAlias) {
			return validateAndCreateSlideWindow(
				(SlideWithSizeAndSlideOnTimeWithAlias) window,
				windowName,
				timeField);
		} else if (window instanceof SessionWithGapOnTimeWithAlias) {
			return validateAndCreateSessionWindow(
				(SessionWithGapOnTimeWithAlias) window,
				windowName,
				timeField);
		} else {
			throw new TableException("Unknown window type: " + window);
		}
	}

	private FieldReferenceExpression getValidatedTimeAttribute(GroupWindow window, ExpressionResolver resolver) {
		List<ResolvedExpression> timeFieldExprs = resolver.resolve(singletonList(window.getTimeField()));

		if (timeFieldExprs.size() != 1) {
			throw new ValidationException("A group window only supports a single time field column.");
		}

		Expression timeFieldExpr = timeFieldExprs.get(0);
		if (!(timeFieldExpr instanceof FieldReferenceExpression)) {
			throw new ValidationException("A group window expects a time attribute for grouping.");
		}

		FieldReferenceExpression timeField = (FieldReferenceExpression) timeFieldExpr;

		final LogicalType timeFieldType = timeField.getOutputDataType().getLogicalType();

		validateTimeAttributeType(timeFieldType);

		return timeField;
	}

	private void validateTimeAttributeType(LogicalType timeFieldType) {
		if (isStreamingMode) {
			validateStreamTimeAttribute(timeFieldType);
		} else {
			validateBatchTimeAttribute(timeFieldType);
		}
	}

	private void validateBatchTimeAttribute(LogicalType timeFieldType) {
		if (!(hasRoot(timeFieldType, TIMESTAMP_WITHOUT_TIME_ZONE) || hasRoot(timeFieldType, BIGINT))) {
			throw new ValidationException("A group window expects a time attribute for grouping " +
				"in a batch environment.");
		}
	}

	private void validateStreamTimeAttribute(LogicalType timeFieldType) {
		if (!hasRoot(timeFieldType, TIMESTAMP_WITHOUT_TIME_ZONE) || !isTimeAttribute(timeFieldType)) {
			throw new ValidationException("A group window expects a time attribute for grouping " +
				"in a stream environment.");
		}
	}

	private ResolvedGroupWindow validateAndCreateTumbleWindow(
			TumbleWithSizeOnTimeWithAlias window,
			String windowName,
			FieldReferenceExpression timeField) {
		ValueLiteralExpression windowSize = getAsValueLiteral(window.getSize(),
			"A tumble window expects a size value literal.");

		final LogicalType timeFieldType = timeField.getOutputDataType().getLogicalType();
		final LogicalType windowSizeType = windowSize.getOutputDataType().getLogicalType();

		if (!hasRoot(windowSizeType, BIGINT) && !hasRoot(windowSizeType, INTERVAL_DAY_TIME)) {
			throw new ValidationException(
				"Tumbling window expects a size literal of a day-time interval or BIGINT type.");
		}

		validateWindowIntervalType(timeFieldType, windowSizeType);

		return ResolvedGroupWindow.tumblingWindow(
			windowName,
			timeField,
			windowSize);
	}

	private ResolvedGroupWindow validateAndCreateSlideWindow(
			SlideWithSizeAndSlideOnTimeWithAlias window,
			String windowName,
			FieldReferenceExpression timeField) {
		ValueLiteralExpression windowSize = getAsValueLiteral(window.getSize(),
			"A sliding window expects a size value literal.");
		ValueLiteralExpression windowSlide = getAsValueLiteral(window.getSlide(),
			"A sliding window expects a slide value literal.");

		final LogicalType timeFieldType = timeField.getOutputDataType().getLogicalType();
		final LogicalType windowSizeType = windowSize.getOutputDataType().getLogicalType();
		final LogicalType windowSlideType = windowSlide.getOutputDataType().getLogicalType();

		if (!hasRoot(windowSizeType, BIGINT) && !hasRoot(windowSizeType, INTERVAL_DAY_TIME)) {
			throw new ValidationException(
				"A sliding window expects a size literal of a day-time interval or BIGINT type.");
		}

		if (!windowSizeType.equals(windowSlideType)) {
			throw new ValidationException("A sliding window expects the same type of size and slide.");
		}

		validateWindowIntervalType(timeFieldType, windowSizeType);

		return ResolvedGroupWindow.slidingWindow(
			windowName,
			timeField,
			windowSize,
			windowSlide);
	}

	private ResolvedGroupWindow validateAndCreateSessionWindow(
			SessionWithGapOnTimeWithAlias window,
			String windowName,
			FieldReferenceExpression timeField) {
		ValueLiteralExpression windowGap = getAsValueLiteral(
			window.getGap(),
			"A session window expects a gap value literal.");

		final LogicalType windowGapType = windowGap.getOutputDataType().getLogicalType();

		if (!hasRoot(windowGapType, INTERVAL_DAY_TIME)) {
			throw new ValidationException("A session window expects a gap literal of a day-time interval type.");
		}

		return ResolvedGroupWindow.sessionWindow(
			windowName,
			timeField,
			windowGap);
	}

	private void validateWindowIntervalType(LogicalType timeFieldType, LogicalType intervalType) {
		if (hasRoot(intervalType, TIMESTAMP_WITHOUT_TIME_ZONE) && isRowtimeAttribute(timeFieldType) &&
				hasRoot(intervalType, BIGINT)) {
			// unsupported row intervals on event-time
			throw new ValidationException(
				"Event-time grouping windows on row intervals in a stream environment " +
					"are currently not supported.");
		}
	}

	private ValueLiteralExpression getAsValueLiteral(Expression expression, String exceptionMessage) {
		if (!(expression instanceof ValueLiteralExpression)) {
			throw new ValidationException(exceptionMessage);
		}
		return (ValueLiteralExpression) expression;
	}

	private void validateWindowProperties(List<ResolvedExpression> windowProperties, ResolvedGroupWindow window) {
		if (!windowProperties.isEmpty()) {
			if (window.getType() == TUMBLE || window.getType() == SLIDE) {
				DataType windowType = window.getSize().get().getOutputDataType();
				if (LogicalTypeChecks.hasRoot(windowType.getLogicalType(), BIGINT)) {
					throw new ValidationException(String.format("Window start and Window end cannot be selected " +
						"for a row-count %s window.", window.getType().toString().toLowerCase()));
				}
			}
		}
	}

	private static <T> Stream<T> concat(Stream<T> first, Stream<T> second, Stream<T> third) {
		Stream<T> firstConcat = Stream.concat(first, second);
		return Stream.concat(firstConcat, third);
	}

	private void validateGroupings(List<ResolvedExpression> groupings) {
		groupings.forEach(expr -> expr.getOutputDataType().getLogicalType().accept(isKeyTypeChecker));
	}

	private void validateAggregates(List<ResolvedExpression> aggregates) {
		aggregates.forEach(agg -> agg.accept(aggregationsValidator));
	}

	private class AggregationExpressionValidator extends ResolvedExpressionDefaultVisitor<Void> {

		@Override
		public Void visit(CallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			if (isFunctionOfKind(call, AGGREGATE) || isFunctionOfKind(call, TABLE_AGGREGATE)) {
				if (functionDefinition == BuiltInFunctionDefinitions.DISTINCT) {
					call.getChildren().forEach(expr -> expr.accept(validateDistinct));
				} else {
					if (requiresOver(functionDefinition)) {
						throw new ValidationException(format(
							"OVER clause is necessary for window functions: [%s].", call));
					}

					call.getChildren().forEach(child -> child.accept(noNestedAggregates));
				}
			} else if (functionDefinition == AS) {
				// skip alias
				call.getChildren().get(0).accept(this);
			} else {
				failExpression(call);
			}
			return null;
		}

		private boolean requiresOver(FunctionDefinition functionDefinition) {
			return functionDefinition.getRequirements()
				.contains(FunctionRequirement.OVER_WINDOW_ONLY);
		}

		@Override
		protected Void defaultMethod(ResolvedExpression expression) {
			failExpression(expression);
			return null;
		}

		protected void failExpression(ResolvedExpression expression) {
			throw new ValidationException(format("Expression '%s' is invalid because it is neither" +
				" present in GROUP BY nor an aggregate function", expression));
		}
	}

	private class ValidateDistinct extends ResolvedExpressionDefaultVisitor<Void> {

		@Override
		public Void visit(CallExpression call) {
			if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.DISTINCT) {
				throw new ValidationException("It's not allowed to use an aggregate function as " +
					"input of another aggregate function");
			} else if (!isFunctionOfKind(call, AGGREGATE) && !isFunctionOfKind(call, TABLE_AGGREGATE)) {
				throw new ValidationException("Distinct operator can only be applied to aggregation expressions!");
			} else {
				call.getChildren().forEach(child -> child.accept(noNestedAggregates));
			}
			return null;
		}

		@Override
		protected Void defaultMethod(ResolvedExpression expression) {
			return null;
		}
	}

	private class NoNestedAggregates extends ResolvedExpressionDefaultVisitor<Void> {

		@Override
		public Void visit(CallExpression call) {
			if (isFunctionOfKind(call, AGGREGATE) || isFunctionOfKind(call, TABLE_AGGREGATE)) {
				throw new ValidationException("It's not allowed to use an aggregate function as " +
					"input of another aggregate function");
			}
			call.getChildren().forEach(expr -> expr.accept(this));
			return null;
		}

		@Override
		protected Void defaultMethod(ResolvedExpression expression) {
			return null;
		}
	}

	private static class IsKeyTypeChecker extends LogicalTypeDefaultVisitor<Boolean> {

		@Override
		public Boolean visit(StructuredType structuredType) {
			StructuredType.StructuredComparision comparision = structuredType.getComparision();
			return comparision == StructuredType.StructuredComparision.FULL ||
				comparision == StructuredType.StructuredComparision.EQUALS;
		}

		@Override
		protected Boolean defaultMethod(LogicalType logicalType) {
			if (logicalType.getTypeRoot() == LogicalTypeRoot.RAW) {
				// we don't know anything about the RAW type, we don't know if it is comparable and hashable.
				return false;
			} else if (logicalType instanceof LegacyTypeInformationType) {
				return ((LegacyTypeInformationType) logicalType).getTypeInformation().isKeyType();
			}

			return logicalType.getChildren().stream().allMatch(c -> c.accept(this));
		}
	}

	/**
	 * Extract a table aggregate Expression and it's aliases.
	 */
	Tuple2<ResolvedExpression, List<String>> extractTableAggFunctionAndAliases(Expression callExpr) {
		TableAggFunctionCallResolver visitor = new TableAggFunctionCallResolver();
		return Tuple2.of(callExpr.accept(visitor), visitor.getAlias());
	}

	private static class TableAggFunctionCallResolver extends ResolvedExpressionDefaultVisitor<ResolvedExpression> {

		private List<String> alias = new LinkedList<>();

		public List<String> getAlias() {
			return alias;
		}

		@Override
		public ResolvedExpression visit(CallExpression call) {
			FunctionDefinition definition = call.getFunctionDefinition();
			if (definition == BuiltInFunctionDefinitions.AS) {
				return unwrapFromAlias(call);
			} else if (isFunctionOfKind(call, TABLE_AGGREGATE)) {
				return call;
			} else {
				return defaultMethod(call);
			}
		}

		private ResolvedExpression unwrapFromAlias(CallExpression call) {
			List<ResolvedExpression> children = call.getResolvedChildren();
			List<String> aliases = children.subList(1, children.size())
				.stream()
				.map(alias -> ExpressionUtils.extractValue(alias, String.class)
					.orElseThrow(() -> new ValidationException("Unexpected alias: " + alias)))
				.collect(toList());

			if (!isFunctionOfKind(children.get(0), TABLE_AGGREGATE)) {
				throw fail();
			}

			validateAlias(
				aliases,
				(TableAggregateFunctionDefinition) ((CallExpression) children.get(0)).getFunctionDefinition());
			alias = aliases;
			return children.get(0);
		}

		private void validateAlias(
			List<String> aliases,
			TableAggregateFunctionDefinition aggFunctionDefinition) {

			TypeInformation<?> resultType = aggFunctionDefinition.getResultTypeInfo();

			int callArity = resultType.getTotalFields();
			int aliasesSize = aliases.size();

			if (aliasesSize > 0 && aliasesSize != callArity) {
				throw new ValidationException(String.format(
					"List of column aliases must have same degree as table; " +
						"the returned table of function '%s' has " +
						"%d columns, whereas alias list has %d columns",
					aggFunctionDefinition,
					callArity,
					aliasesSize));
			}
		}

		@Override
		protected ResolvedExpression defaultMethod(ResolvedExpression expression) {
			throw fail();
		}

		private ValidationException fail() {
			return new ValidationException(
				"A flatAggregate only accepts an expression which defines a table aggregate " +
					"function that might be followed by some alias.");
		}
	}
}
