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

package org.apache.flink.table.expressions.resolver;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.range;
import static org.apache.flink.table.api.Expressions.withColumns;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * This test supports only a subset of builtin functions because those functions still depend on
 * planner expressions for argument validation and type inference. Supported builtin functions are:
 *
 * <p>- BuiltinFunctionDefinitions.EQUALS
 * - BuiltinFunctionDefinitions.IS_NULL
 *
 * <p>Pseudo functions that are executed during expression resolution e.g.:
 * - BuiltinFunctionDefinitions.WITH_COLUMNS
 * - BuiltinFunctionDefinitions.WITHOUT_COLUMNS
 * - BuiltinFunctionDefinitions.RANGE_TO
 * - BuiltinFunctionDefinitions.FLATTEN
 *
 * <p>This test supports only a simplified identifier parsing logic. It does not support escaping.
 * It just naively splits on dots. The proper logic comes with a planner implementation which is not
 * available in the API module.
 */
@RunWith(Parameterized.class)
public class ExpressionResolverTest {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<TestSpec> parameters() {
		return Arrays.asList(
			TestSpec.test("Columns range")
				.inputSchemas(
					TableSchema.builder()
						.field("f0", DataTypes.BIGINT())
						.field("f1", DataTypes.STRING())
						.field("f2", DataTypes.SMALLINT())
						.build()
				)
				.select(withColumns(range("f1", "f2")), withColumns(range(1, 2)))
				.equalTo(
					new FieldReferenceExpression("f1", DataTypes.STRING(), 0, 1),
					new FieldReferenceExpression("f2", DataTypes.SMALLINT(), 0, 2),
					new FieldReferenceExpression("f0", DataTypes.BIGINT(), 0, 0),
					new FieldReferenceExpression("f1", DataTypes.STRING(), 0, 1)
				),

			TestSpec.test("Flatten call")
				.inputSchemas(
					TableSchema.builder()
						.field("f0", DataTypes.ROW(
							DataTypes.FIELD("n0", DataTypes.BIGINT()),
							DataTypes.FIELD("n1", DataTypes.STRING())
						))
						.build()
				)
				.select($("f0").flatten())
				.equalTo(
					new CallExpression(
						FunctionIdentifier.of("get"),
						BuiltInFunctionDefinitions.GET,
						Arrays.asList(
							new FieldReferenceExpression("f0", DataTypes.ROW(
								DataTypes.FIELD("n0", DataTypes.BIGINT()),
								DataTypes.FIELD("n1", DataTypes.STRING())
							), 0, 0),
							new ValueLiteralExpression("n0")
						),
						DataTypes.BIGINT()
					),
					new CallExpression(
						FunctionIdentifier.of("get"),
						BuiltInFunctionDefinitions.GET,
						Arrays.asList(
							new FieldReferenceExpression("f0", DataTypes.ROW(
								DataTypes.FIELD("n0", DataTypes.BIGINT()),
								DataTypes.FIELD("n1", DataTypes.STRING())
							), 0, 0),
							new ValueLiteralExpression("n1")
						),
						DataTypes.STRING()
					)),

			TestSpec.test("Builtin function calls")
				.inputSchemas(
					TableSchema.builder()
						.field("f0", DataTypes.INT())
						.field("f1", DataTypes.STRING())
						.build()
				)
				.select($("f0").isEqual($("f1")))
				.equalTo(
					new CallExpression(
						FunctionIdentifier.of("equals"),
						BuiltInFunctionDefinitions.EQUALS,
						Arrays.asList(
							new FieldReferenceExpression("f0", DataTypes.INT(), 0, 0),
							new FieldReferenceExpression("f1", DataTypes.STRING(), 0, 1)
						),
						DataTypes.BOOLEAN()
					)),

			TestSpec.test("Lookup legacy scalar function call")
				.inputSchemas(
					TableSchema.builder()
						.field("f0", DataTypes.INT())
						.build()
				)
				.lookupFunction("func", new ScalarFunctionDefinition("func", new LegacyScalarFunc()))
				.select(call("func", 1, $("f0")))
				.equalTo(
					new CallExpression(
						FunctionIdentifier.of("func"),
						new ScalarFunctionDefinition("func", new LegacyScalarFunc()),
						Arrays.asList(valueLiteral(1), new FieldReferenceExpression("f0", DataTypes.INT(), 0, 0)),
						DataTypes.INT().bridgedTo(Integer.class)
					)),

			TestSpec.test("Lookup system function call")
				.inputSchemas(
					TableSchema.builder()
						.field("f0", DataTypes.INT())
						.build()
				)
				.lookupFunction("func", new ScalarFunc())
				.select(call("func", 1, $("f0")))
				.equalTo(
					new CallExpression(
						FunctionIdentifier.of("func"),
						new ScalarFunc(),
						Arrays.asList(valueLiteral(1), new FieldReferenceExpression("f0", DataTypes.INT(), 0, 0)),
						DataTypes.INT().notNull().bridgedTo(int.class)
					)),

			TestSpec.test("Lookup catalog function call")
				.inputSchemas(
					TableSchema.builder()
						.field("f0", DataTypes.INT())
						.build()
				)
				.lookupFunction(ObjectIdentifier.of("cat", "db", "func"), new ScalarFunc())
				.select(call("cat.db.func", 1, $("f0")))
				.equalTo(
					new CallExpression(
						FunctionIdentifier.of(ObjectIdentifier.of("cat", "db", "func")),
						new ScalarFunc(),
						Arrays.asList(valueLiteral(1), new FieldReferenceExpression("f0", DataTypes.INT(), 0, 0)),
						DataTypes.INT().notNull().bridgedTo(int.class)
					)),

			TestSpec.test("Deeply nested user-defined inline calls")
				.inputSchemas(
					TableSchema.builder()
						.field("f0", DataTypes.INT())
						.build()
				)
				.lookupFunction("func", new ScalarFunc())
				.select(call("func", call(new ScalarFunc(), call("func", 1, $("f0")))))
				.equalTo(
					new CallExpression(
						FunctionIdentifier.of("func"),
						new ScalarFunc(),
						Collections.singletonList(
							new CallExpression(
								new ScalarFunc(),
								Collections.singletonList(new CallExpression(
									FunctionIdentifier.of("func"),
									new ScalarFunc(),
									Arrays.asList(
										valueLiteral(1),
										new FieldReferenceExpression("f0", DataTypes.INT(), 0, 0)),
									DataTypes.INT().notNull().bridgedTo(int.class)
								)),
								DataTypes.INT().notNull().bridgedTo(int.class)
							)),
						DataTypes.INT().notNull().bridgedTo(int.class))
				)
		);
	}

	@Parameterized.Parameter
	public TestSpec testSpec;

	@Test
	public void testResolvingExpressions() {
		List<ResolvedExpression> resolvedExpressions = testSpec.getResolver()
			.resolve(Arrays.asList(testSpec.expressions));
		assertThat(
			resolvedExpressions,
			equalTo(testSpec.expectedExpressions));
	}

	/**
	 * Test scalar function.
	 */
	@FunctionHint(
		input = @DataTypeHint(inputGroup = InputGroup.ANY),
		isVarArgs = true,
		output = @DataTypeHint(value = "INTEGER NOT NULL", bridgedTo = int.class))
	public static class ScalarFunc extends ScalarFunction {
		public int eval(Object... any) {
			return 0;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof ScalarFunc;
		}
	}

	/**
	 * Legacy scalar function.
	 */
	public static class LegacyScalarFunc extends ScalarFunction {
		public int eval(Object... any) {
			return 0;
		}

		@Override
		public TypeInformation<?> getResultType(Class<?>[] signature) {
			return Types.INT;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof ScalarFunc;
		}
	}

	private static class TestSpec {
		private final String description;
		private TableSchema[] schemas;
		private Expression[] expressions;
		private List<ResolvedExpression> expectedExpressions;
		private Map<FunctionIdentifier, FunctionDefinition> functions = new HashMap<>();

		private TestSpec(String description) {
			this.description = description;
		}

		public static TestSpec test(String description) {
			return new TestSpec(description);
		}

		public TestSpec inputSchemas(TableSchema... schemas) {
			this.schemas = schemas;
			return this;
		}

		public TestSpec lookupFunction(String name, FunctionDefinition functionDefinition) {
			functions.put(FunctionIdentifier.of(name), functionDefinition);
			return this;
		}

		public TestSpec lookupFunction(ObjectIdentifier identifier, FunctionDefinition functionDefinition) {
			functions.put(FunctionIdentifier.of(identifier), functionDefinition);
			return this;
		}

		public TestSpec select(Expression... expressions) {
			this.expressions = expressions;
			return this;
		}

		public TestSpec equalTo(ResolvedExpression... resolvedExpressions) {
			this.expectedExpressions = Arrays.asList(resolvedExpressions);
			return this;
		}

		public ExpressionResolver getResolver() {
			FunctionLookup functionLookup = new FunctionLookup() {
				@Override
				public Optional<Result> lookupFunction(String stringIdentifier) {
					// this is a simplified version for the test
					return lookupFunction(UnresolvedIdentifier.of(stringIdentifier.split("\\.")));
				}

				@Override
				public Optional<Result> lookupFunction(UnresolvedIdentifier identifier) {
					final FunctionIdentifier functionIdentifier;
					if (identifier.getCatalogName().isPresent() && identifier.getDatabaseName().isPresent()) {
						functionIdentifier = FunctionIdentifier.of(
							ObjectIdentifier.of(
								identifier.getCatalogName().get(),
								identifier.getDatabaseName().get(),
								identifier.getObjectName()));
					} else {
						functionIdentifier = FunctionIdentifier.of(identifier.getObjectName());
					}

					return Optional.ofNullable(functions.get(functionIdentifier))
						.map(func -> new Result(functionIdentifier, func));
				}

				@Override
				public Result lookupBuiltInFunction(BuiltInFunctionDefinition definition) {
					return new Result(
						FunctionIdentifier.of(definition.getName()),
						definition
					);
				}

				@Override
				public PlannerTypeInferenceUtil getPlannerTypeInferenceUtil() {
					return (unresolvedCall, resolvedArgs) -> {
						FunctionDefinition functionDefinition = unresolvedCall.getFunctionDefinition();
						if (functionDefinition.equals(BuiltInFunctionDefinitions.EQUALS)) {
							return new TypeInferenceUtil.Result(
								resolvedArgs.stream()
									.map(ResolvedExpression::getOutputDataType)
									.collect(Collectors.toList()),
								null,
								DataTypes.BOOLEAN()
							);
						} else if (functionDefinition.equals(BuiltInFunctionDefinitions.IS_NULL)) {
							return new TypeInferenceUtil.Result(
								resolvedArgs.stream()
									.map(ResolvedExpression::getOutputDataType)
									.collect(Collectors.toList()),
								null,
								DataTypes.BOOLEAN()
							);
						} else if (functionDefinition instanceof ScalarFunctionDefinition) {
							return new TypeInferenceUtil.Result(
								resolvedArgs.stream()
									.map(ResolvedExpression::getOutputDataType)
									.collect(Collectors.toList()),
								null,
								// We do not support a full legacy type inference here. We support only a static result
								// type
								TypeConversions.fromLegacyInfoToDataType(((ScalarFunctionDefinition) functionDefinition)
									.getScalarFunction()
									.getResultType(null)));
						}

						throw new IllegalArgumentException(
							"Unsupported builtin function in the test: " + unresolvedCall);
					};
				}
			};
			return ExpressionResolver.resolverFor(
				new TableConfig(),
				name -> Optional.empty(),
				functionLookup,
				new DataTypeFactoryMock(),
				Arrays.stream(schemas)
					.map(schema -> (QueryOperation) new CatalogQueryOperation(ObjectIdentifier.of("", "", ""), schema))
					.toArray(QueryOperation[]::new)
			).build();
		}

		@Override
		public String toString() {
			return description;
		}
	}
}
