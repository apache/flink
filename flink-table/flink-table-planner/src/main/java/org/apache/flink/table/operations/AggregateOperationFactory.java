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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.SessionWithGapOnTimeWithAlias;
import org.apache.flink.table.api.SlideWithSizeAndSlideOnTimeWithAlias;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TumbleWithSizeOnTimeWithAlias;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.AggFunctionCall;
import org.apache.flink.table.expressions.AggregateFunctionDefinition;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.ExpressionResolver;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.operations.WindowAggregateTableOperation.ResolvedGroupWindow;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.expressions.ExpressionUtils.isFunctionOfType;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;
import static org.apache.flink.table.operations.OperationExpressionsUtils.extractName;
import static org.apache.flink.table.operations.WindowAggregateTableOperation.ResolvedGroupWindow.WindowType.SLIDE;
import static org.apache.flink.table.operations.WindowAggregateTableOperation.ResolvedGroupWindow.WindowType.TUMBLE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Utility class for creating a valid {@link AggregateTableOperation} or {@link WindowAggregateTableOperation}.
 */
@Internal
public class AggregateOperationFactory {

	private final boolean isStreaming;
	private final ExpressionBridge<PlannerExpression> expressionBridge;
	private final GroupingExpressionValidator groupingExpressionValidator = new GroupingExpressionValidator();
	private final NoNestedAggregates noNestedAggregates = new NoNestedAggregates();
	private final ValidateDistinct validateDistinct = new ValidateDistinct();
	private AggregationExpressionValidator aggregationsValidator = new AggregationExpressionValidator();

	public AggregateOperationFactory(ExpressionBridge<PlannerExpression> expressionBridge, boolean isStreaming) {
		this.expressionBridge = expressionBridge;
		this.isStreaming = isStreaming;
	}

	/**
	 * Creates a valid {@link AggregateTableOperation} operation.
	 *
	 * @param groupings expressions describing grouping key of aggregates
	 * @param aggregates expressions describing aggregation functions
	 * @param child relational operation on top of which to apply the aggregation
	 * @return valid aggregate operation
	 */
	public TableOperation createAggregate(
			List<Expression> groupings,
			List<Expression> aggregates,
			TableOperation child) {
		validateGroupings(groupings);
		validateAggregates(aggregates);

		List<PlannerExpression> convertedGroupings = bridge(groupings);
		List<PlannerExpression> convertedAggregates = bridge(aggregates);

		TypeInformation[] fieldTypes = Stream.concat(
			convertedGroupings.stream().map(PlannerExpression::resultType),
			convertedAggregates.stream().flatMap(this::extractAggregateResultTypes)
		).toArray(TypeInformation[]::new);

		String[] fieldNames = Stream.concat(
			groupings.stream().map(expr -> extractName(expr).orElseGet(expr::toString)),
			aggregates.stream().flatMap(this::extractAggregateNames)
		).toArray(String[]::new);

		TableSchema tableSchema = new TableSchema(fieldNames, fieldTypes);

		return new AggregateTableOperation(groupings, aggregates, child, tableSchema);
	}

	/**
	 * Creates a valid {@link WindowAggregateTableOperation} operation.
	 *
	 * @param groupings expressions describing grouping key of aggregates
	 * @param aggregates expressions describing aggregation functions
	 * @param windowProperties expressions describing window properties
	 * @param window grouping window of this aggregation
	 * @param child relational operation on top of which to apply the aggregation
	 * @return valid window aggregate operation
	 */
	public TableOperation createWindowAggregate(
			List<Expression> groupings,
			List<Expression> aggregates,
			List<Expression> windowProperties,
			ResolvedGroupWindow window,
			TableOperation child) {
		validateGroupings(groupings);
		validateAggregates(aggregates);
		validateWindowProperties(windowProperties, window);

		List<PlannerExpression> convertedGroupings = bridge(groupings);
		List<PlannerExpression> convertedAggregates = bridge(aggregates);
		List<PlannerExpression> convertedWindowProperties = bridge(windowProperties);

		TypeInformation[] fieldTypes = concat(
			convertedGroupings.stream().map(PlannerExpression::resultType),
			convertedAggregates.stream().flatMap(this::extractAggregateResultTypes),
			convertedWindowProperties.stream().map(PlannerExpression::resultType)
		).toArray(TypeInformation[]::new);

		String[] fieldNames = concat(
			groupings.stream().map(expr -> extractName(expr).orElseGet(expr::toString)),
			aggregates.stream().flatMap(this::extractAggregateNames),
			windowProperties.stream().map(expr -> extractName(expr).orElseGet(expr::toString))
		).toArray(String[]::new);

		TableSchema tableSchema = new TableSchema(fieldNames, fieldTypes);

		return new WindowAggregateTableOperation(
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
	private Stream<TypeInformation<?>> extractAggregateResultTypes(PlannerExpression plannerExpression) {
		if (plannerExpression instanceof AggFunctionCall &&
			((AggFunctionCall) plannerExpression).aggregateFunction() instanceof TableAggregateFunction) {
			return Stream.of(UserDefinedFunctionUtils.getFieldInfo(plannerExpression.resultType())._3());
		} else {
			return Stream.of(plannerExpression.resultType());
		}
	}

	/**
	 * Extract names for the aggregate or the table aggregate expression. For a table aggregate, it
	 * may return multi output names when the composite return type is flattened.
	 */
	private Stream<String> extractAggregateNames(Expression expression) {
		if (isTableAggFunctionCall(expression)) {
			return Arrays.stream(UserDefinedFunctionUtils.getFieldInfo(
				((AggregateFunctionDefinition) ((CallExpression) expression).getFunctionDefinition())
					.getResultTypeInfo())._1());
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
	public ResolvedGroupWindow createResolvedWindow(GroupWindow window, ExpressionResolver resolver) {
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
		List<Expression> timeFieldExprs = resolver.resolve(singletonList(window.getTimeField()));

		if (timeFieldExprs.size() != 1) {
			throw new ValidationException("A group window only supports a single time field column.");
		}

		Expression timeFieldExpr = timeFieldExprs.get(0);
		if (!(timeFieldExpr instanceof FieldReferenceExpression)) {
			throw new ValidationException("A group window expects a time attribute for grouping.");
		}

		FieldReferenceExpression timeField = (FieldReferenceExpression) timeFieldExpr;
		validateTimeAttributeType(timeField);
		return timeField;
	}

	private void validateTimeAttributeType(FieldReferenceExpression timeField) {
		TypeInformation<?> timeFieldType = timeField.getResultType();
		if (isStreaming) {
			validateStreamTimeAttribute(timeFieldType);
		} else {
			validateBatchTimeAttribute(timeFieldType);
		}
	}

	private void validateBatchTimeAttribute(TypeInformation<?> timeFieldType) {
		if (!(timeFieldType instanceof SqlTimeTypeInfo || timeFieldType == LONG_TYPE_INFO)) {
			throw new ValidationException("A group window expects a time attribute for grouping " +
				"in a batch environment.");
		}
	}

	private void validateStreamTimeAttribute(TypeInformation<?> timeFieldType) {
		if (!(timeFieldType instanceof TimeIndicatorTypeInfo)) {
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

		final LogicalType windowSizeType = windowSize.getDataType().getLogicalType();

		if (!hasRoot(windowSizeType, BIGINT) && !hasRoot(windowSizeType, INTERVAL_DAY_TIME)) {
			throw new ValidationException(
				"Tumbling window expects a size literal of a day-time interval or BIGINT type.");
		}

		validateWindowIntervalType(timeField, windowSizeType);

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

		final LogicalType windowSizeType = windowSize.getDataType().getLogicalType();
		final LogicalType windowSlideType = windowSlide.getDataType().getLogicalType();

		if (!hasRoot(windowSizeType, BIGINT) && !hasRoot(windowSizeType, INTERVAL_DAY_TIME)) {
			throw new ValidationException(
				"A sliding window expects a size literal of a day-time interval or BIGINT type.");
		}

		if (!windowSizeType.equals(windowSlideType)) {
			throw new ValidationException("A sliding window expects the same type of size and slide.");
		}

		validateWindowIntervalType(timeField, windowSizeType);

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

		final LogicalType windowGapType = windowGap.getDataType().getLogicalType();

		if (!hasRoot(windowGapType, INTERVAL_DAY_TIME)) {
			throw new ValidationException("A session window expects a gap literal of a day-time interval type.");
		}

		return ResolvedGroupWindow.sessionWindow(
			windowName,
			timeField,
			windowGap);
	}

	private void validateWindowIntervalType(FieldReferenceExpression timeField, LogicalType intervalType) {
		if (isRowTimeIndicator(timeField) && hasRoot(intervalType, BIGINT)) {
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

	private boolean isRowTimeIndicator(FieldReferenceExpression field) {
		return field.getResultType() instanceof TimeIndicatorTypeInfo &&
			((TimeIndicatorTypeInfo) field.getResultType()).isEventTime();
	}

	private void validateWindowProperties(List<Expression> windowProperties, ResolvedGroupWindow window) {
		if (!windowProperties.isEmpty()) {
			if (window.getType() == TUMBLE || window.getType() == SLIDE) {
				TypeInformation<?> resultType = window.getSize().map(expressionBridge::bridge).get().resultType();
				if (resultType == LONG_TYPE_INFO) {
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

	private List<PlannerExpression> bridge(List<Expression> aggregates) {
		return aggregates.stream()
			.map(expressionBridge::bridge)
			.collect(Collectors.toList());
	}

	private void validateGroupings(List<Expression> groupings) {
		groupings.forEach(expr -> expr.accept(groupingExpressionValidator));
	}

	private void validateAggregates(List<Expression> aggregates) {
		aggregates.forEach(agg -> agg.accept(aggregationsValidator));
	}

	private class AggregationExpressionValidator extends ApiExpressionDefaultVisitor<Void> {

		@Override
		public Void visitCall(CallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			if (isFunctionOfType(call, AGGREGATE_FUNCTION)) {
				if (functionDefinition == BuiltInFunctionDefinitions.DISTINCT) {
					call.getChildren().forEach(expr -> expr.accept(validateDistinct));
				} else {
					if (functionDefinition instanceof AggregateFunctionDefinition) {
						if (requiresOver(functionDefinition)) {
							throw new ValidationException(format(
								"OVER clause is necessary for window functions: [%s].",
								call));
						}
					}

					call.getChildren().forEach(child -> child.accept(noNestedAggregates));
				}
			} else if (functionDefinition == BuiltInFunctionDefinitions.AS) {
				// skip alias
				call.getChildren().get(0).accept(this);
			} else {
				failExpression(call);
			}
			return null;
		}

		private boolean requiresOver(FunctionDefinition functionDefinition) {
			return ((AggregateFunctionDefinition) functionDefinition).getAggregateFunction()
				instanceof AggregateFunction &&
				((AggregateFunction) ((AggregateFunctionDefinition) functionDefinition)
					.getAggregateFunction()).requiresOver();
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			failExpression(expression);
			return null;
		}

		protected void failExpression(Expression expression) {
			throw new ValidationException(format("Expression '%s' is invalid because it is neither" +
				" present in GROUP BY nor an aggregate function", expression));
		}
	}

	private class ValidateDistinct extends ApiExpressionDefaultVisitor<Void> {

		@Override
		public Void visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == BuiltInFunctionDefinitions.DISTINCT) {
				throw new ValidationException("It's not allowed to use an aggregate function as " +
					"input of another aggregate function");
			} else if (call.getFunctionDefinition().getType() != AGGREGATE_FUNCTION) {
				throw new ValidationException("Distinct operator can only be applied to aggregation expressions!");
			} else {
				call.getChildren().forEach(child -> child.accept(noNestedAggregates));
			}
			return null;
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			return null;
		}
	}

	private class NoNestedAggregates extends ApiExpressionDefaultVisitor<Void> {

		@Override
		public Void visitCall(CallExpression call) {
			if (call.getFunctionDefinition().getType() == AGGREGATE_FUNCTION) {
				throw new ValidationException("It's not allowed to use an aggregate function as " +
					"input of another aggregate function");
			}
			call.getChildren().forEach(expr -> expr.accept(this));
			return null;
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			return null;
		}
	}

	private class GroupingExpressionValidator extends ApiExpressionDefaultVisitor<Void> {
		@Override
		protected Void defaultMethod(Expression expression) {
			TypeInformation<?> groupingType = expressionBridge.bridge(expression).resultType();

			if (!groupingType.isKeyType()) {
				throw new ValidationException(format("Expression %s cannot be used as a grouping expression " +
					"because it's not a valid key type which must be hashable and comparable", expression));
			}
			return null;
		}
	}

	/**
	 * Extract a table aggregate Expression and it's aliases.
	 */
	public Tuple2<Expression, List<String>> extractTableAggFunctionAndAliases(Expression callExpr) {
		TableAggFunctionCallVisitor visitor = new TableAggFunctionCallVisitor();
		return Tuple2.of(callExpr.accept(visitor), visitor.getAlias());
	}

	private class TableAggFunctionCallVisitor extends ApiExpressionDefaultVisitor<Expression> {

		private List<String> alias = new LinkedList<>();

		public List<String> getAlias() {
			return alias;
		}

		@Override
		public Expression visitCall(CallExpression call) {
			FunctionDefinition definition = call.getFunctionDefinition();
			if (definition.equals(AS)) {
				return unwrapFromAlias(call);
			} else if (definition instanceof AggregateFunctionDefinition) {
				if (!isTableAggFunctionCall(call)) {
					throw fail();
				}
				return call;
			} else {
				return defaultMethod(call);
			}
		}

		private Expression unwrapFromAlias(CallExpression call) {
			List<Expression> children = call.getChildren();
			List<String> aliases = children.subList(1, children.size())
				.stream()
				.map(alias -> ExpressionUtils.extractValue(alias, String.class)
					.orElseThrow(() -> new ValidationException("Unexpected alias: " + alias)))
				.collect(toList());

			if (!isTableAggFunctionCall(children.get(0))) {
				throw fail();
			}

			validateAlias(aliases, (AggregateFunctionDefinition) ((CallExpression) children.get(0)).getFunctionDefinition());
			alias = aliases;
			return children.get(0);
		}

		private void validateAlias(
			List<String> aliases,
			AggregateFunctionDefinition aggFunctionDefinition) {

			TypeInformation<?> resultType = aggFunctionDefinition.getResultTypeInfo();

			int callArity = resultType.getTotalFields();
			int aliasesSize = aliases.size();

			if (aliasesSize > 0 && aliasesSize != callArity) {
				throw new ValidationException(String.format(
					"List of column aliases must have same degree as table; " +
						"the returned table of function '%s' has " +
						"%d columns, whereas alias list has %d columns",
					aggFunctionDefinition.getName(),
					callArity,
					aliasesSize));
			}
		}

		@Override
		protected AggFunctionCall defaultMethod(Expression expression) {
			throw fail();
		}

		private ValidationException fail() {
			return new ValidationException(
				"A flatAggregate only accepts an expression which defines a table aggregate " +
					"function that might be followed by some alias.");
		}
	}

	/**
	 * Return true if the input {@link Expression} is a {@link CallExpression} of table aggregate function.
	 */
	public static boolean isTableAggFunctionCall(Expression expression) {
		return Stream.of(expression)
			.filter(p -> p instanceof CallExpression)
			.map(p -> (CallExpression) p)
			.filter(p -> p.getFunctionDefinition() instanceof AggregateFunctionDefinition)
			.map(p -> (AggregateFunctionDefinition) p.getFunctionDefinition())
			.anyMatch(p -> p.getAggregateFunction() instanceof TableAggregateFunction);
	}
}
