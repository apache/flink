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
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.SessionWithGapOnTimeWithAlias;
import org.apache.flink.table.api.SlideWithSizeAndSlideOnTimeWithAlias;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TumbleWithSizeOnTimeWithAlias;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.AggregateFunctionDefinition;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionResolver;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.ExpressionUtils.isFunctionOfType;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;
import static org.apache.flink.table.operations.OperationExpressionsUtils.extractName;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isTimeAttribute;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Utility class for creating a valid {@link AggregateQueryOperation} or {@link WindowAggregateQueryOperation}.
 */
@Internal
public class AggregateOperationFactory {

	private final boolean isStreaming;
	private final GroupingExpressionValidator groupingExpressionValidator = new GroupingExpressionValidator();
	private final NoNestedAggregates noNestedAggregates = new NoNestedAggregates();
	private final ValidateDistinct validateDistinct = new ValidateDistinct();
	private AggregationExpressionValidator aggregationsValidator = new AggregationExpressionValidator();

	public AggregateOperationFactory(boolean isStreaming) {
		this.isStreaming = isStreaming;
	}

	/**
	 * Creates a valid {@link AggregateQueryOperation} operation.
	 *
	 * @param groupings expressions describing grouping key of aggregates
	 * @param aggregates expressions describing aggregation functions
	 * @param child relational operation on top of which to apply the aggregation
	 * @return valid aggregate operation
	 */
	public QueryOperation createAggregate(
			List<Expression> groupings,
			List<Expression> aggregates,
			QueryOperation child) {
		validateGroupings(groupings);
		validateAggregates(aggregates);

		DataType[] fieldTypes = Stream.concat(groupings.stream(), aggregates.stream())
				.map(ExpressionTypeInfer::infer).toArray(DataType[]::new);

		String[] fieldNames = Stream.concat(
				groupings.stream().map(expr -> extractName(expr).orElseGet(expr::toString)),
				aggregates.stream().flatMap(this::extractAggregateNames)
		).toArray(String[]::new);

		TableSchema tableSchema = TableSchema.builder().fields(fieldNames, fieldTypes).build();

		return new AggregateQueryOperation(groupings, aggregates, child, tableSchema);
	}

	/**
	 * Extract names for the aggregate or the table aggregate expression. For a table aggregate, it
	 * may return multi output names when the composite return type is flattened.
	 */
	private Stream<String> extractAggregateNames(Expression expression) {
		if (isTableAggFunctionCall(expression)) {
			return Arrays.stream(UserDefinedFunctionUtils.getFieldInfo(fromLegacyInfoToDataType(
					((AggregateFunctionDefinition) ((CallExpression) expression).getFunctionDefinition())
							.getResultTypeInfo()))._1());
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

		final LogicalType timeFieldType = timeField.getOutputDataType().getLogicalType();

		validateTimeAttributeType(timeFieldType);

		return timeField;
	}

	private void validateTimeAttributeType(LogicalType timeFieldType) {
		if (isStreaming) {
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
			TypeInformation<?> groupingType = fromDataTypeToLegacyInfo(ExpressionTypeInfer.infer(expression));

			if (!groupingType.isKeyType()) {
				throw new ValidationException(format("Expression %s cannot be used as a grouping expression " +
						"because it's not a valid key type which must be hashable and comparable", expression));
			}
			return null;
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
