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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility class for creating valid alias expressions that can be later used as a projection.
 */
@Internal
public final class AliasOperationUtils {

	private static final AliasLiteralValidator aliasLiteralValidator = new AliasLiteralValidator();
	private static final String ALL_REFERENCE = "*";

	/**
	 * Creates a list of valid alias expressions. Resulting expression might still contain
	 * {@link UnresolvedReferenceExpression}.
	 *
	 * @param aliases aliases to validate
	 * @param child relational operation on top of which to apply the aliases
	 * @return validated list of aliases
	 */
	public static List<Expression> createAliasList(List<Expression> aliases, TableOperation child) {
		TableSchema childSchema = child.getTableSchema();

		if (aliases.size() > childSchema.getFieldCount()) {
			throw new ValidationException("Aliasing more fields than we actually have.");
		}

		List<ValueLiteralExpression> fieldAliases = aliases.stream()
			.map(f -> f.accept(aliasLiteralValidator))
			.collect(Collectors.toList());

		String[] childNames = childSchema.getFieldNames();
		return IntStream.range(0, childNames.length)
			.mapToObj(idx -> {
				UnresolvedReferenceExpression oldField = new UnresolvedReferenceExpression(childNames[idx]);
				if (idx < fieldAliases.size()) {
					ValueLiteralExpression alias = fieldAliases.get(idx);
					return new CallExpression(BuiltInFunctionDefinitions.AS, Arrays.asList(oldField, alias));
				} else {
					return oldField;
				}
			}).collect(Collectors.toList());
	}

	private static class AliasLiteralValidator extends ApiExpressionDefaultVisitor<ValueLiteralExpression> {

		@Override
		public ValueLiteralExpression visitValueLiteral(ValueLiteralExpression valueLiteralExpression) {
			String name = ApiExpressionUtils.extractValue(valueLiteralExpression, Types.STRING())
				.orElseThrow(() -> new ValidationException(
					"Alias accepts only names that are not '*' reference."));

			if (name.equals(ALL_REFERENCE)) {
				throw new ValidationException("Alias can not accept '*' as name.");
			}
			return valueLiteralExpression;
		}

		@Override
		protected ValueLiteralExpression defaultMethod(Expression expression) {
			throw new ValidationException("Alias accepts only names that are not '*' reference.");
		}

		@Override
		public ValueLiteralExpression visitUnresolvedReference(UnresolvedReferenceExpression unresolvedReference) {

			if (unresolvedReference.getName().equals(ALL_REFERENCE)) {
				throw new ValidationException("Alias can not accept '*' as name.");
			}
			return new ValueLiteralExpression(unresolvedReference.getName());
		}
	}

	private AliasOperationUtils() {
	}
}
