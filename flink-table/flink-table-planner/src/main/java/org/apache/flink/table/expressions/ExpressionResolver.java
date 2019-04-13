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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.SessionWithGapOnTimeWithAlias;
import org.apache.flink.table.api.SlideWithSizeAndSlideOnTimeWithAlias;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TumbleWithSizeOnTimeWithAlias;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.catalog.FunctionDefinitionCatalog;
import org.apache.flink.table.expressions.lookups.FieldReferenceLookup;
import org.apache.flink.table.expressions.lookups.TableReferenceLookup;
import org.apache.flink.table.expressions.rules.ResolverRule;
import org.apache.flink.table.expressions.rules.ResolverRules;
import org.apache.flink.table.operations.TableOperation;
import org.apache.flink.table.plan.logical.LogicalOverWindow;
import org.apache.flink.table.plan.logical.LogicalWindow;
import org.apache.flink.table.plan.logical.SessionGroupWindow;
import org.apache.flink.table.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.plan.logical.TumblingGroupWindow;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Some;

import static java.util.stream.Collectors.toList;

/**
 * Tries to resolve all unresolved expressions such as {@link UnresolvedReferenceExpression}
 * or calls such as {@link BuiltInFunctionDefinitions#OVER}.
 *
 * <p>The default set of rules ({@link ExpressionResolver#getResolverRules()}) will resolve following references:
 * <ul>
 *     <li>flatten '*' to all fields of underlying inputs</li>
 *     <li>join over aggregates with corresponding over windows into a single resolved call</li>
 *     <li>resolve remaining unresolved references to fields, tables or local references</li>
 *     <li>replace call to {@link BuiltInFunctionDefinitions#FLATTEN}</li>
 *     <li>performs call arguments types validation and inserts additional casts if possible</li>
 * </ul>
 */
@Internal
public class ExpressionResolver {

	/**
	 * List of rules that will be applied during expression resolution.
	 */
	public static List<ResolverRule> getResolverRules() {
		return Arrays.asList(
			ResolverRules.LOOKUP_CALL_BY_NAME,
			ResolverRules.FLATTEN_STAR_REFERENCE,
			ResolverRules.OVER_WINDOWS,
			ResolverRules.FIELD_RESOLVE,
			ResolverRules.FLATTEN_CALL,
			ResolverRules.RESOLVE_CALL_BY_ARGUMENTS);
	}

	private final PlannerExpressionConverter bridgeConverter = PlannerExpressionConverter.INSTANCE();

	private final FieldReferenceLookup fieldLookup;

	private final TableReferenceLookup tableLookup;

	private final FunctionDefinitionCatalog functionLookup;

	//TODO change to LocalReferenceLookup, once we don't need to resolve fields to create LocalReferenceExpression
	private final Map<String, LocalReferenceExpression> localReferences = new HashMap<>();

	private final Map<Expression, LogicalOverWindow> overWindows;

	private final Function<List<Expression>, List<Expression>> resolveFunction;

	private ExpressionResolver(
			TableReferenceLookup tableLookup,
			FunctionDefinitionCatalog functionCatalog,
			FieldReferenceLookup fieldLookup,
			List<OverWindow> overWindows,
			@Nullable GroupWindow groupWindow,
			List<ResolverRule> rules) {
		this.tableLookup = Preconditions.checkNotNull(tableLookup);
		this.fieldLookup = Preconditions.checkNotNull(fieldLookup);
		this.functionLookup = Preconditions.checkNotNull(functionCatalog);
		this.resolveFunction = concatenateRules(rules);

		this.overWindows = prepareOverWindows(overWindows);
		prepareLocalReferencesFromGroupWindows(groupWindow);
	}

	/**
	 * Creates a builder for {@link ExpressionResolver}. One can add additional properties to the resolver
	 * like e.g. {@link GroupWindow} or {@link OverWindow}. You can also add additional {@link ResolverRule}.
	 *
	 * @param tableCatalog a way to lookup a table reference by name
	 * @param functionDefinitionCatalog a way to lookup call by name
	 * @param inputs inputs to use for field resolution
	 * @return builder for resolver
	 */
	public static ExpressionResolverBuilder resolverFor(
			TableReferenceLookup tableCatalog,
			FunctionDefinitionCatalog functionDefinitionCatalog,
			TableOperation... inputs) {
		return new ExpressionResolverBuilder(inputs, tableCatalog, functionDefinitionCatalog);
	}

	/**
	 * Resolves given expressions with configured set of rules. All expressions of an operation should be
	 * given at once as some rules might assume the order of expressions.
	 *
	 * <p>After this method is applied the returned expressions should be ready to be converted to planner specific
	 * expressions.
	 *
	 * @param expressions list of expressions to resolve.
	 * @return resolved list of expression
	 */
	public List<Expression> resolve(List<Expression> expressions) {
		return resolveFunction.apply(expressions);
	}

	/**
	 * Converts an API class to a logical window for planning with expressions already resolved.
	 *
	 * @param window window to resolve
	 * @return logical window with expressions resolved
	 */
	public LogicalWindow resolveGroupWindow(GroupWindow window) {
		Expression alias = window.getAlias();

		if (!(alias instanceof UnresolvedReferenceExpression)) {
			throw new ValidationException("Alias of group window should be an UnresolvedFieldReference");
		}

		final String windowName = ((UnresolvedReferenceExpression) alias).getName();
		PlannerExpression timeField = resolveFieldsInSingleExpression(window.getTimeField()).accept(bridgeConverter);

		//TODO replace with LocalReferenceExpression
		WindowReference resolvedAlias = new WindowReference(windowName, new Some<>(timeField.resultType()));

		if (window instanceof TumbleWithSizeOnTimeWithAlias) {
			TumbleWithSizeOnTimeWithAlias tw = (TumbleWithSizeOnTimeWithAlias) window;
			return new TumblingGroupWindow(
				resolvedAlias,
				timeField,
				resolveFieldsInSingleExpression(tw.getSize()).accept(bridgeConverter));
		} else if (window instanceof SlideWithSizeAndSlideOnTimeWithAlias) {
			SlideWithSizeAndSlideOnTimeWithAlias sw = (SlideWithSizeAndSlideOnTimeWithAlias) window;
			return new SlidingGroupWindow(
				resolvedAlias,
				timeField,
				resolveFieldsInSingleExpression(sw.getSize()).accept(bridgeConverter),
				resolveFieldsInSingleExpression(sw.getSlide()).accept(bridgeConverter));
		} else if (window instanceof SessionWithGapOnTimeWithAlias) {
			SessionWithGapOnTimeWithAlias sw = (SessionWithGapOnTimeWithAlias) window;
			return new SessionGroupWindow(
				resolvedAlias,
				timeField,
				resolveFieldsInSingleExpression(sw.getGap()).accept(bridgeConverter));
		} else {
			throw new TableException("Unknown window type");
		}
	}

	private Function<List<Expression>, List<Expression>> concatenateRules(List<ResolverRule> rules) {
		return rules.stream()
			.reduce(
				Function.identity(),
				(function, resolverRule) -> function.andThen(exprs -> resolverRule.apply(exprs,
					new ExpressionResolverContext())),
				Function::andThen
			);
	}

	private void prepareLocalReferencesFromGroupWindows(@Nullable GroupWindow groupWindow) {
		if (groupWindow != null) {
			String windowName = ((UnresolvedReferenceExpression) groupWindow.getAlias()).getName();
			TypeInformation<?> windowType = resolveFieldsInSingleExpression(groupWindow.getTimeField())
				.accept(bridgeConverter)
				.resultType();

			localReferences.put(windowName, new LocalReferenceExpression(windowName, windowType));
		}
	}

	private Map<Expression, LogicalOverWindow> prepareOverWindows(List<OverWindow> overWindows) {
		return overWindows.stream()
			.map(this::resolveOverWindow)
			.collect(Collectors.toMap(
				LogicalOverWindow::alias,
				Function.identity()
			));
	}

	private Expression resolveFieldsInSingleExpression(Expression expression) {
		List<Expression> expressions = ResolverRules.FIELD_RESOLVE.apply(Collections.singletonList(expression),
			new ExpressionResolverContext());

		if (expressions.size() != 1) {
			throw new TableException("Expected a single expression as a result. Got: " + expressions);
		}

		return expressions.get(0);
	}

	private class ExpressionResolverContext implements ResolverRule.ResolutionContext {

		@Override
		public FieldReferenceLookup referenceLookup() {
			return fieldLookup;
		}

		@Override
		public TableReferenceLookup tableLookup() {
			return tableLookup;
		}

		@Override
		public FunctionDefinitionCatalog functionDefinitionLookup() {
			return functionLookup;
		}

		@Override
		public Optional<LocalReferenceExpression> getLocalReference(String alias) {
			return Optional.ofNullable(localReferences.get(alias));
		}

		@Override
		public Optional<LogicalOverWindow> getOverWindow(Expression alias) {
			return Optional.ofNullable(overWindows.get(alias));
		}

		@Override
		public PlannerExpression bridge(Expression expression) {
			return expression.accept(bridgeConverter);
		}
	}

	private LogicalOverWindow resolveOverWindow(OverWindow overWindow) {
		return new LogicalOverWindow(
			overWindow.getAlias(),
			overWindow.getPartitioning().stream().map(this::resolveFieldsInSingleExpression).collect(toList()),
			resolveFieldsInSingleExpression(overWindow.getOrder()),
			resolveFieldsInSingleExpression(overWindow.getPreceding()),
			overWindow.getFollowing().map(this::resolveFieldsInSingleExpression)
		);
	}

	/**
	 * Builder for creating {@link ExpressionResolver}.
	 */
	public static class ExpressionResolverBuilder {

		private final List<TableOperation> tableOperations;
		private final TableReferenceLookup tableCatalog;
		private final FunctionDefinitionCatalog functionCatalog;
		private List<OverWindow> logicalOverWindows = new ArrayList<>();
		private GroupWindow groupWindow = null;
		private List<ResolverRule> rules = new ArrayList<>(getResolverRules());

		private ExpressionResolverBuilder(
				TableOperation[] tableOperations,
				TableReferenceLookup tableCatalog,
				FunctionDefinitionCatalog functionCatalog) {
			this.tableOperations = Arrays.asList(tableOperations);
			this.tableCatalog = tableCatalog;
			this.functionCatalog = functionCatalog;
		}

		public ExpressionResolverBuilder withOverWindows(List<OverWindow> windows) {
			this.logicalOverWindows = Preconditions.checkNotNull(windows);
			return this;
		}

		public ExpressionResolverBuilder withGroupWindow(GroupWindow window) {
			this.groupWindow = Preconditions.checkNotNull(window);
			return this;
		}

		public ExpressionResolver build() {
			return new ExpressionResolver(
				tableCatalog,
				functionCatalog,
				new FieldReferenceLookup(tableOperations),
				logicalOverWindows,
				groupWindow,
				rules);
		}
	}
}
