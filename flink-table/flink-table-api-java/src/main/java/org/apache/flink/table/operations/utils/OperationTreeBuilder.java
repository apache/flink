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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.expressions.resolver.LookupCallResolver;
import org.apache.flink.table.expressions.resolver.lookups.TableReferenceLookup;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.utils.ApiExpressionUtils;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.operations.DistinctQueryOperation;
import org.apache.flink.table.operations.FilterQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation.JoinType;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow;
import org.apache.flink.table.operations.utils.factories.AggregateOperationFactory;
import org.apache.flink.table.operations.utils.factories.AliasOperationUtils;
import org.apache.flink.table.operations.utils.factories.CalculatedTableFactory;
import org.apache.flink.table.operations.utils.factories.ColumnOperationUtils;
import org.apache.flink.table.operations.utils.factories.JoinOperationFactory;
import org.apache.flink.table.operations.utils.factories.ProjectionOperationFactory;
import org.apache.flink.table.operations.utils.factories.SetOperationFactory;
import org.apache.flink.table.operations.utils.factories.SortOperationFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.FieldInfoUtils;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.operations.SetQueryOperation.SetQueryOperationType.INTERSECT;
import static org.apache.flink.table.operations.SetQueryOperation.SetQueryOperationType.MINUS;
import static org.apache.flink.table.operations.SetQueryOperation.SetQueryOperationType.UNION;

/**
 * A builder for constructing validated {@link QueryOperation}s.
 */
@Internal
public final class OperationTreeBuilder {

	private final TableConfig config;
	private final FunctionLookup functionCatalog;
	private final TableReferenceLookup tableReferenceLookup;
	private final LookupCallResolver lookupResolver;

	/**
	 * Utility classes for constructing a validated operation of certain type.
	 */
	private final ProjectionOperationFactory projectionOperationFactory;
	private final SortOperationFactory sortOperationFactory;
	private final CalculatedTableFactory calculatedTableFactory;
	private final SetOperationFactory setOperationFactory;
	private final AggregateOperationFactory aggregateOperationFactory;
	private final JoinOperationFactory joinOperationFactory;

	private OperationTreeBuilder(
			TableConfig config,
			FunctionLookup functionLookup,
			TableReferenceLookup tableReferenceLookup,
			ProjectionOperationFactory projectionOperationFactory,
			SortOperationFactory sortOperationFactory,
			CalculatedTableFactory calculatedTableFactory,
			SetOperationFactory setOperationFactory,
			AggregateOperationFactory aggregateOperationFactory,
			JoinOperationFactory joinOperationFactory) {
		this.config = config;
		this.functionCatalog = functionLookup;
		this.tableReferenceLookup = tableReferenceLookup;
		this.projectionOperationFactory = projectionOperationFactory;
		this.sortOperationFactory = sortOperationFactory;
		this.calculatedTableFactory = calculatedTableFactory;
		this.setOperationFactory = setOperationFactory;
		this.aggregateOperationFactory = aggregateOperationFactory;
		this.joinOperationFactory = joinOperationFactory;
		this.lookupResolver = new LookupCallResolver(functionLookup);
	}

	public static OperationTreeBuilder create(
			TableConfig config,
			FunctionLookup functionCatalog,
			TableReferenceLookup tableReferenceLookup,
			boolean isStreamingMode) {
		return new OperationTreeBuilder(
			config,
			functionCatalog,
			tableReferenceLookup,
			new ProjectionOperationFactory(),
			new SortOperationFactory(isStreamingMode),
			new CalculatedTableFactory(),
			new SetOperationFactory(isStreamingMode),
			new AggregateOperationFactory(isStreamingMode),
			new JoinOperationFactory()
		);
	}

	public QueryOperation project(List<Expression> projectList, QueryOperation child) {
		return project(projectList, child, false);
	}

	public QueryOperation project(List<Expression> projectList, QueryOperation child, boolean explicitAlias) {
		projectList.forEach(p -> p.accept(new NoAggregateChecker(
			"Aggregate functions are not supported in the select right after the aggregate" +
				" or flatAggregate operation.")));
		projectList.forEach(p -> p.accept(new NoWindowPropertyChecker(
			"Window properties can only be used on windowed tables.")));
		return projectInternal(projectList, child, explicitAlias, Collections.emptyList());
	}

	public QueryOperation project(List<Expression> projectList, QueryOperation child, List<OverWindow> overWindows) {

		Preconditions.checkArgument(!overWindows.isEmpty());

		projectList.forEach(p -> p.accept(new NoWindowPropertyChecker(
			"Window start and end properties are not available for Over windows.")));

		return projectInternal(
			projectList,
			child,
			true,
			overWindows);
	}

	private QueryOperation projectInternal(
			List<Expression> projectList,
			QueryOperation child,
			boolean explicitAlias,
			List<OverWindow> overWindows) {

		ExpressionResolver resolver = ExpressionResolver.resolverFor(
				config,
				tableReferenceLookup,
				functionCatalog,
				child)
			.withOverWindows(overWindows)
			.build();
		List<ResolvedExpression> projections = resolver.resolve(projectList);
		return projectionOperationFactory.create(projections, child, explicitAlias, resolver.postResolverFactory());
	}

	/**
	 * Adds additional columns. Existing fields will be replaced if replaceIfExist is true.
	 */
	public QueryOperation addColumns(boolean replaceIfExist, List<Expression> fieldLists, QueryOperation child) {
		final List<Expression> newColumns;
		if (replaceIfExist) {
			String[] fieldNames = child.getTableSchema().getFieldNames();
			newColumns = ColumnOperationUtils.addOrReplaceColumns(Arrays.asList(fieldNames), fieldLists);
		} else {
			newColumns = new ArrayList<>(fieldLists);
			newColumns.add(0, new UnresolvedReferenceExpression("*"));
		}
		return project(newColumns, child, false);
	}

	public QueryOperation renameColumns(List<Expression> aliases, QueryOperation child) {

		ExpressionResolver resolver = getResolver(child);
		String[] inputFieldNames = child.getTableSchema().getFieldNames();
		List<Expression> validateAliases = ColumnOperationUtils.renameColumns(
			Arrays.asList(inputFieldNames),
			resolver.resolveExpanding(aliases));

		return project(validateAliases, child, false);
	}

	public QueryOperation dropColumns(List<Expression> fieldLists, QueryOperation child) {

		ExpressionResolver resolver = getResolver(child);
		String[] inputFieldNames = child.getTableSchema().getFieldNames();
		List<Expression> finalFields = ColumnOperationUtils.dropFields(
			Arrays.asList(inputFieldNames),
			resolver.resolveExpanding(fieldLists));

		return project(finalFields, child, false);
	}

	public QueryOperation aggregate(List<Expression> groupingExpressions, List<Expression> aggregates, QueryOperation child) {

		ExpressionResolver resolver = getResolver(child);

		List<ResolvedExpression> resolvedGroupings = resolver.resolve(groupingExpressions);
		List<ResolvedExpression> resolvedAggregates = resolver.resolve(aggregates);

		return aggregateOperationFactory.createAggregate(resolvedGroupings, resolvedAggregates, child);
	}

	public QueryOperation windowAggregate(
		List<Expression> groupingExpressions,
		GroupWindow window,
		List<Expression> windowProperties,
		List<Expression> aggregates,
		QueryOperation child) {

		ExpressionResolver resolver = getResolver(child);
		ResolvedGroupWindow resolvedWindow = aggregateOperationFactory.createResolvedWindow(window, resolver);

		ExpressionResolver resolverWithWindowReferences = ExpressionResolver.resolverFor(
				config,
				tableReferenceLookup,
				functionCatalog,
				child)
			.withLocalReferences(
				new LocalReferenceExpression(
					resolvedWindow.getAlias(),
					resolvedWindow.getTimeAttribute().getOutputDataType()))
			.build();

		List<ResolvedExpression> convertedGroupings = resolverWithWindowReferences.resolve(groupingExpressions);
		List<ResolvedExpression> convertedAggregates = resolverWithWindowReferences.resolve(aggregates);
		List<ResolvedExpression> convertedProperties = resolverWithWindowReferences.resolve(windowProperties);

		return aggregateOperationFactory.createWindowAggregate(
			convertedGroupings,
			convertedAggregates,
			convertedProperties,
			resolvedWindow,
			child);
	}

	public QueryOperation windowAggregate(
		List<Expression> groupingExpressions,
		GroupWindow window,
		List<Expression> windowProperties,
		Expression aggregateFunction,
		QueryOperation child) {

		ExpressionResolver resolver = getResolver(child);
		Expression resolvedAggregate = aggregateFunction.accept(lookupResolver);
		AggregateWithAlias aggregateWithAlias = resolvedAggregate.accept(new ExtractAliasAndAggregate(true, resolver));

		List<Expression> groupsAndAggregate = new ArrayList<>(groupingExpressions);
		groupsAndAggregate.add(aggregateWithAlias.aggregate);
		List<Expression> namedGroupsAndAggregate = addAliasToTheCallInAggregate(
			Arrays.asList(child.getTableSchema().getFieldNames()),
			groupsAndAggregate);

		// Step1: add a default name to the call in the grouping expressions, e.g., groupBy(a % 5) to
		// groupBy(a % 5 as TMP_0). We need a name for every column so that to perform alias for the
		// table aggregate function in Step6.
		List<Expression> newGroupingExpressions = namedGroupsAndAggregate.subList(0, groupingExpressions.size());

		// Step2: turn agg to a named agg, because it will be verified later.
		Expression aggregateRenamed = namedGroupsAndAggregate.get(groupingExpressions.size());

		// Step3: resolve expressions, including grouping, aggregates and window properties.
		ResolvedGroupWindow resolvedWindow = aggregateOperationFactory.createResolvedWindow(window, resolver);
		ExpressionResolver resolverWithWindowReferences = ExpressionResolver.resolverFor(
				config,
				tableReferenceLookup,
				functionCatalog,
				child)
			.withLocalReferences(
				new LocalReferenceExpression(
					resolvedWindow.getAlias(),
					resolvedWindow.getTimeAttribute().getOutputDataType()))
			.build();

		List<ResolvedExpression> convertedGroupings = resolverWithWindowReferences.resolve(newGroupingExpressions);
		List<ResolvedExpression> convertedAggregates = resolverWithWindowReferences.resolve(Collections.singletonList(
			aggregateRenamed));
		List<ResolvedExpression> convertedProperties = resolverWithWindowReferences.resolve(windowProperties);

		// Step4: create window agg operation
		QueryOperation aggregateOperation = aggregateOperationFactory.createWindowAggregate(
			convertedGroupings,
			Collections.singletonList(convertedAggregates.get(0)),
			convertedProperties,
			resolvedWindow,
			child);

		// Step5: flatten the aggregate function
		String[] aggNames = aggregateOperation.getTableSchema().getFieldNames();
		List<Expression> flattenedExpressions = Arrays.stream(aggNames)
			.map(ApiExpressionUtils::unresolvedRef)
			.collect(Collectors.toCollection(ArrayList::new));
		flattenedExpressions.set(
			groupingExpressions.size(),
			unresolvedCall(
				BuiltInFunctionDefinitions.FLATTEN,
				unresolvedRef(aggNames[groupingExpressions.size()])));
		QueryOperation flattenedProjection = this.project(flattenedExpressions, aggregateOperation);

		// Step6: add a top project to alias the output fields of the aggregate. Also, project the
		// window attribute.
		return aliasBackwardFields(flattenedProjection, aggregateWithAlias.aliases, groupingExpressions.size());
	}

	public QueryOperation join(
			QueryOperation left,
			QueryOperation right,
			JoinType joinType,
			Optional<Expression> condition,
			boolean correlated) {
		ExpressionResolver resolver = ExpressionResolver.resolverFor(
				config,
				tableReferenceLookup,
				functionCatalog,
				left,
				right)
			.build();
		Optional<ResolvedExpression> resolvedCondition = condition.map(expr -> resolveSingleExpression(expr, resolver));

		return joinOperationFactory.create(
			left,
			right,
			joinType,
			resolvedCondition.orElse(valueLiteral(true)),
			correlated);
	}

	public QueryOperation joinLateral(
			QueryOperation left,
			Expression tableFunction,
			JoinType joinType,
			Optional<Expression> condition) {
		ExpressionResolver resolver = getResolver(left);
		ResolvedExpression resolvedFunction = resolveSingleExpression(tableFunction, resolver);

		QueryOperation temporalTable = calculatedTableFactory.create(
			resolvedFunction,
			left.getTableSchema().getFieldNames());

		return join(left, temporalTable, joinType, condition, true);
	}

	public Expression resolveExpression(Expression expression, QueryOperation... tableOperation) {
		ExpressionResolver resolver = ExpressionResolver.resolverFor(
			config,
			tableReferenceLookup,
			functionCatalog,
			tableOperation).build();

		return resolveSingleExpression(expression, resolver);
	}

	private ResolvedExpression resolveSingleExpression(Expression expression, ExpressionResolver resolver) {
		List<ResolvedExpression> resolvedExpression = resolver.resolve(Collections.singletonList(expression));
		if (resolvedExpression.size() != 1) {
			throw new ValidationException("Expected single expression");
		} else {
			return resolvedExpression.get(0);
		}
	}

	public QueryOperation sort(List<Expression> fields, QueryOperation child) {

		ExpressionResolver resolver = getResolver(child);
		List<ResolvedExpression> resolvedFields = resolver.resolve(fields);

		return sortOperationFactory.createSort(resolvedFields, child, resolver.postResolverFactory());
	}

	public QueryOperation limitWithOffset(int offset, QueryOperation child) {
		return sortOperationFactory.createLimitWithOffset(offset, child);
	}

	public QueryOperation limitWithFetch(int fetch, QueryOperation child) {
		return sortOperationFactory.createLimitWithFetch(fetch, child);
	}

	public QueryOperation alias(List<Expression> fields, QueryOperation child) {
		List<Expression> newFields = AliasOperationUtils.createAliasList(fields, child);

		return project(newFields, child, true);
	}

	public QueryOperation filter(Expression condition, QueryOperation child) {

		ExpressionResolver resolver = getResolver(child);
		ResolvedExpression resolvedExpression = resolveSingleExpression(condition, resolver);
		DataType conditionType = resolvedExpression.getOutputDataType();
		if (!LogicalTypeChecks.hasRoot(conditionType.getLogicalType(), LogicalTypeRoot.BOOLEAN)) {
			throw new ValidationException("Filter operator requires a boolean expression as input," +
				" but $condition is of type " + conditionType);
		}

		return new FilterQueryOperation(resolvedExpression, child);
	}

	public QueryOperation distinct(QueryOperation child) {
		return new DistinctQueryOperation(child);
	}

	public QueryOperation minus(QueryOperation left, QueryOperation right, boolean all) {
		return setOperationFactory.create(MINUS, left, right, all);
	}

	public QueryOperation intersect(QueryOperation left, QueryOperation right, boolean all) {
		return setOperationFactory.create(INTERSECT, left, right, all);
	}

	public QueryOperation union(QueryOperation left, QueryOperation right, boolean all) {
		return setOperationFactory.create(UNION, left, right, all);
	}

	public QueryOperation map(Expression mapFunction, QueryOperation child) {

		Expression resolvedMapFunction = mapFunction.accept(lookupResolver);

		if (!ApiExpressionUtils.isFunctionOfKind(resolvedMapFunction, FunctionKind.SCALAR)) {
			throw new ValidationException("Only a scalar function can be used in the map operator.");
		}

		Expression expandedFields = unresolvedCall(BuiltInFunctionDefinitions.FLATTEN, resolvedMapFunction);
		return project(Collections.singletonList(expandedFields), child, false);
	}

	public QueryOperation flatMap(Expression tableFunction, QueryOperation child) {

		Expression resolvedTableFunction = tableFunction.accept(lookupResolver);

		if (!ApiExpressionUtils.isFunctionOfKind(resolvedTableFunction, FunctionKind.TABLE)) {
			throw new ValidationException("Only a table function can be used in the flatMap operator.");
		}

		TypeInformation<?> resultType = ((TableFunctionDefinition) ((UnresolvedCallExpression) resolvedTableFunction)
			.getFunctionDefinition())
			.getResultType();
		List<String> originFieldNames = Arrays.asList(FieldInfoUtils.getFieldNames(resultType));

		List<String> childFields = Arrays.asList(child.getTableSchema().getFieldNames());
		Set<String> usedFieldNames = new HashSet<>(childFields);

		List<Expression> args = new ArrayList<>();
		for (String originFieldName : originFieldNames) {
			String resultName = getUniqueName(originFieldName, usedFieldNames);
			usedFieldNames.add(resultName);
			args.add(valueLiteral(resultName));
		}

		args.add(0, resolvedTableFunction);
		Expression renamedTableFunction = unresolvedCall(
			BuiltInFunctionDefinitions.AS,
			args.toArray(new Expression[0]));
		QueryOperation joinNode = joinLateral(child, renamedTableFunction, JoinType.INNER, Optional.empty());
		QueryOperation rightNode = dropColumns(
			childFields.stream().map(UnresolvedReferenceExpression::new).collect(Collectors.toList()),
			joinNode);
		return alias(
			originFieldNames.stream().map(UnresolvedReferenceExpression::new).collect(Collectors.toList()),
			rightNode);
	}

	public QueryOperation aggregate(List<Expression> groupingExpressions, Expression aggregate, QueryOperation child) {
		Expression resolvedAggregate = aggregate.accept(lookupResolver);
		AggregateWithAlias aggregateWithAlias =
			resolvedAggregate.accept(new ExtractAliasAndAggregate(true, getResolver(child)));

		List<Expression> groupsAndAggregate = new ArrayList<>(groupingExpressions);
		groupsAndAggregate.add(aggregateWithAlias.aggregate);
		List<Expression> namedGroupsAndAggregate = addAliasToTheCallInAggregate(
			Arrays.asList(child.getTableSchema().getFieldNames()),
			groupsAndAggregate);

		// Step1: add a default name to the call in the grouping expressions, e.g., groupBy(a % 5) to
		// groupBy(a % 5 as TMP_0). We need a name for every column so that to perform alias for the
		// aggregate function in Step5.
		List<Expression> newGroupingExpressions = namedGroupsAndAggregate.subList(0, groupingExpressions.size());

		// Step2: turn agg to a named agg, because it will be verified later.
		Expression aggregateRenamed = namedGroupsAndAggregate.get(groupingExpressions.size());

		// Step3: get agg table
		QueryOperation aggregateOperation = this.aggregate(
			newGroupingExpressions,
			Collections.singletonList(aggregateRenamed),
			child);

		// Step4: flatten the aggregate function
		String[] aggNames = aggregateOperation.getTableSchema().getFieldNames();
		List<Expression> flattenedExpressions = Arrays.asList(aggNames)
			.subList(0, groupingExpressions.size())
			.stream()
			.map(ApiExpressionUtils::unresolvedRef)
			.collect(Collectors.toCollection(ArrayList::new));

		flattenedExpressions.add(unresolvedCall(
			BuiltInFunctionDefinitions.FLATTEN,
			unresolvedRef(aggNames[aggNames.length - 1])));

		QueryOperation flattenedProjection = this.project(flattenedExpressions, aggregateOperation);

		// Step5: add alias
		return aliasBackwardFields(flattenedProjection, aggregateWithAlias.aliases, groupingExpressions.size());
	}

	private static class AggregateWithAlias {
		private final UnresolvedCallExpression aggregate;
		private final List<String> aliases;

		private AggregateWithAlias(UnresolvedCallExpression aggregate, List<String> aliases) {
			this.aggregate = aggregate;
			this.aliases = aliases;
		}
	}

	private static class ExtractAliasAndAggregate extends ApiExpressionDefaultVisitor<AggregateWithAlias> {

		// need this flag to validate alias, i.e., the length of alias and function result type should be same.
		private boolean isRowbasedAggregate = false;
		private ExpressionResolver resolver = null;

		public ExtractAliasAndAggregate(boolean isRowbasedAggregate, ExpressionResolver resolver) {
			this.isRowbasedAggregate = isRowbasedAggregate;
			this.resolver = resolver;
		}

		@Override
		public AggregateWithAlias visit(UnresolvedCallExpression unresolvedCall) {
			if (ApiExpressionUtils.isFunction(unresolvedCall, BuiltInFunctionDefinitions.AS)) {
				Expression expression = unresolvedCall.getChildren().get(0);
				if (expression instanceof UnresolvedCallExpression) {
					List<String> aliases = extractAliases(unresolvedCall);

					return getAggregate((UnresolvedCallExpression) expression, aliases)
						.orElseGet(() -> defaultMethod(unresolvedCall));
				} else {
					return defaultMethod(unresolvedCall);
				}
			}

			return getAggregate(unresolvedCall, Collections.emptyList()).orElseGet(() -> defaultMethod(unresolvedCall));
		}

		private List<String> extractAliases(UnresolvedCallExpression unresolvedCall) {
			return unresolvedCall.getChildren()
								.subList(1, unresolvedCall.getChildren().size())
								.stream()
								.map(ex -> ExpressionUtils.extractValue(ex, String.class)
									.orElseThrow(() -> new TableException("Expected string literal as alias.")))
								.collect(Collectors.toList());
		}

		private Optional<AggregateWithAlias> getAggregate(
				UnresolvedCallExpression unresolvedCall,
				List<String> aliases) {
			FunctionDefinition functionDefinition = unresolvedCall.getFunctionDefinition();
			if (ApiExpressionUtils.isFunctionOfKind(unresolvedCall, FunctionKind.AGGREGATE)) {
				final List<String> fieldNames;
				if (aliases.isEmpty()) {
					if (functionDefinition instanceof AggregateFunctionDefinition) {
						TypeInformation<?> resultTypeInfo = ((AggregateFunctionDefinition) functionDefinition)
							.getResultTypeInfo();
						fieldNames = Arrays.asList(FieldInfoUtils.getFieldNames(resultTypeInfo));
					} else {
						fieldNames = Collections.emptyList();
					}
				} else {
					ResolvedExpression resolvedExpression =
						resolver.resolve(Collections.singletonList(unresolvedCall)).get(0);
					validateAlias(aliases, resolvedExpression, isRowbasedAggregate);
					fieldNames = aliases;
				}
				return Optional.of(new AggregateWithAlias(unresolvedCall, fieldNames));
			} else {
				return Optional.empty();
			}
		}

		@Override
		protected AggregateWithAlias defaultMethod(Expression expression) {
			throw new ValidationException("Aggregate function expected. Got: " + expression);
		}

		private void validateAlias(
			List<String> aliases,
			ResolvedExpression resolvedExpression,
			Boolean isRowbasedAggregate) {

			int length = TypeConversions
				.fromDataTypeToLegacyInfo(resolvedExpression.getOutputDataType()).getArity();
			int callArity = isRowbasedAggregate ? length : 1;
			int aliasesSize = aliases.size();

			if ((0 < aliasesSize) && (aliasesSize != callArity)) {
				throw new ValidationException(String.format(
					"List of column aliases must have same degree as table; " +
						"the returned table of function '%s' has " +
						"%d columns, whereas alias list has %d columns",
					resolvedExpression,
					callArity,
					aliasesSize));
			}
		}
	}

	public QueryOperation tableAggregate(
			List<Expression> groupingExpressions,
			Expression tableAggFunction,
			QueryOperation child) {

		// Step1: add a default name to the call in the grouping expressions, e.g., groupBy(a % 5) to
		// groupBy(a % 5 as TMP_0). We need a name for every column so that to perform alias for the
		// table aggregate function in Step4.
		List<Expression> newGroupingExpressions = addAliasToTheCallInAggregate(
			Arrays.asList(child.getTableSchema().getFieldNames()),
			groupingExpressions);

		// Step2: resolve expressions
		ExpressionResolver resolver = getResolver(child);
		List<ResolvedExpression> resolvedGroupings = resolver.resolve(newGroupingExpressions);
		Tuple2<ResolvedExpression, List<String>> resolvedFunctionAndAlias =
			aggregateOperationFactory.extractTableAggFunctionAndAliases(
				resolveSingleExpression(tableAggFunction, resolver));

		// Step3: create table agg operation
		QueryOperation tableAggOperation = aggregateOperationFactory
			.createAggregate(resolvedGroupings, Collections.singletonList(resolvedFunctionAndAlias.f0), child);

		// Step4: add a top project to alias the output fields of the table aggregate.
		return aliasBackwardFields(tableAggOperation, resolvedFunctionAndAlias.f1, groupingExpressions.size());
	}

	public QueryOperation windowTableAggregate(
			List<Expression> groupingExpressions,
			GroupWindow window,
			List<Expression> windowProperties,
			Expression tableAggFunction,
			QueryOperation child) {

		// Step1: add a default name to the call in the grouping expressions, e.g., groupBy(a % 5) to
		// groupBy(a % 5 as TMP_0). We need a name for every column so that to perform alias for the
		// table aggregate function in Step4.
		List<Expression> newGroupingExpressions = addAliasToTheCallInAggregate(
			Arrays.asList(child.getTableSchema().getFieldNames()),
			groupingExpressions);

		// Step2: resolve expressions, including grouping, aggregates and window properties.
		ExpressionResolver resolver = getResolver(child);
		ResolvedGroupWindow resolvedWindow = aggregateOperationFactory.createResolvedWindow(window, resolver);

		ExpressionResolver resolverWithWindowReferences = ExpressionResolver.resolverFor(
			config,
			tableReferenceLookup,
			functionCatalog,
			child)
			.withLocalReferences(
				new LocalReferenceExpression(
					resolvedWindow.getAlias(),
					resolvedWindow.getTimeAttribute().getOutputDataType()))
			.build();

		List<ResolvedExpression> convertedGroupings = resolverWithWindowReferences.resolve(newGroupingExpressions);
		List<ResolvedExpression> convertedAggregates = resolverWithWindowReferences.resolve(Collections.singletonList(
			tableAggFunction));
		List<ResolvedExpression> convertedProperties = resolverWithWindowReferences.resolve(windowProperties);
		Tuple2<ResolvedExpression, List<String>> resolvedFunctionAndAlias = aggregateOperationFactory
			.extractTableAggFunctionAndAliases(convertedAggregates.get(0));

		// Step3: create window table agg operation
		QueryOperation tableAggOperation = aggregateOperationFactory.createWindowAggregate(
			convertedGroupings,
			Collections.singletonList(resolvedFunctionAndAlias.f0),
			convertedProperties,
			resolvedWindow,
			child);

		// Step4: add a top project to alias the output fields of the table aggregate. Also, project the
		// window attribute.
		return aliasBackwardFields(tableAggOperation, resolvedFunctionAndAlias.f1, groupingExpressions.size());
	}

	/**
	 * Rename fields in the input {@link QueryOperation}.
	 */
	private QueryOperation aliasBackwardFields(
		QueryOperation inputOperation,
		List<String> alias,
		int aliasStartIndex) {

		if (!alias.isEmpty()) {
			String[] namesBeforeAlias = inputOperation.getTableSchema().getFieldNames();
			List<String> namesAfterAlias = new ArrayList<>(Arrays.asList(namesBeforeAlias));
			for (int i = 0; i < alias.size(); i++) {
				int withOffset = aliasStartIndex + i;
				namesAfterAlias.remove(withOffset);
				namesAfterAlias.add(withOffset, alias.get(i));
			}

			return this.alias(namesAfterAlias.stream()
				.map(UnresolvedReferenceExpression::new)
				.collect(Collectors.toList()), inputOperation);
		} else {
			return inputOperation;
		}
	}

	/**
	 * Add a default name to the call in the grouping expressions, e.g., groupBy(a % 5) to
	 * groupBy(a % 5 as TMP_0) or make aggregate a named aggregate.
	 */
	private List<Expression> addAliasToTheCallInAggregate(
		List<String> inputFieldNames,
		List<Expression> expressions) {

		int attrNameCntr = 0;
		Set<String> usedFieldNames = new HashSet<>(inputFieldNames);

		List<Expression> result = new ArrayList<>();
		for (Expression groupingExpression : expressions) {
			if (groupingExpression instanceof UnresolvedCallExpression &&
				!ApiExpressionUtils.isFunction(groupingExpression, BuiltInFunctionDefinitions.AS)) {
				String tempName = getUniqueName("TMP_" + attrNameCntr, usedFieldNames);
				attrNameCntr += 1;
				usedFieldNames.add(tempName);
				result.add(unresolvedCall(
					BuiltInFunctionDefinitions.AS,
					groupingExpression,
					valueLiteral(tempName)));
			} else {
				result.add(groupingExpression);
			}
		}

		return result;
	}

	/**
	 * Return a unique name that does not exist in usedFieldNames according to the input name.
	 */
	private String getUniqueName(String inputName, Collection<String> usedFieldNames) {
		int i = 0;
		String resultName = inputName;
		while (usedFieldNames.contains(resultName)) {
			resultName = resultName + "_" + i;
			i += 1;
		}
		return resultName;
	}

	private ExpressionResolver getResolver(QueryOperation child) {
		return ExpressionResolver.resolverFor(
				config,
				tableReferenceLookup,
				functionCatalog,
				child)
			.build();
	}

	private static class NoWindowPropertyChecker extends ApiExpressionDefaultVisitor<Void> {
		private final String exceptionMessage;

		private NoWindowPropertyChecker(String exceptionMessage) {
			this.exceptionMessage = exceptionMessage;
		}

		@Override
		public Void visit(UnresolvedCallExpression call) {
			FunctionDefinition functionDefinition = call.getFunctionDefinition();
			if (BuiltInFunctionDefinitions.WINDOW_PROPERTIES.contains(functionDefinition)) {
				throw new ValidationException(exceptionMessage);
			}
			call.getChildren().forEach(expr -> expr.accept(this));
			return null;
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			return null;
		}
	}

	private static class NoAggregateChecker extends ApiExpressionDefaultVisitor<Void> {
		private final String exceptionMessage;

		private NoAggregateChecker(String exceptionMessage) {
			this.exceptionMessage = exceptionMessage;
		}

		@Override
		public Void visit(UnresolvedCallExpression call) {
			if (ApiExpressionUtils.isFunctionOfKind(call, FunctionKind.AGGREGATE)) {
				throw new ValidationException(exceptionMessage);
			}
			call.getChildren().forEach(expr -> expr.accept(this));
			return null;
		}

		@Override
		protected Void defaultMethod(Expression expression) {
			return null;
		}
	}
}
