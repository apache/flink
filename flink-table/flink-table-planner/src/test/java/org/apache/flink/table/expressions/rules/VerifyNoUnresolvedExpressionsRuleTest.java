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

package org.apache.flink.table.expressions.rules;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.rules.ResolverRule.ResolutionContext;

import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.lookupCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.COUNT;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.FLATTEN;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.OVER;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link VerifyNoUnresolvedExpressionsRule}.
 */
public class VerifyNoUnresolvedExpressionsRuleTest {

	private static final ResolverRule resolverRule = ResolverRules.VERIFY_NO_MORE_UNRESOLVED_EXPRESSIONS;

	private static final ResolutionContext resolutionContext = mock(ResolutionContext.class);

	@Test(expected = TableException.class)
	public void testUnresolvedReferenceIsCatched() {
		List<Expression> expressions = asList(
			unresolvedRef("field"),
			new FieldReferenceExpression("resolvedField", DataTypes.INT(), 0, 0));
		resolverRule.apply(expressions, resolutionContext);
	}

	@Test(expected = TableException.class)
	public void testNestedUnresolvedReferenceIsCatched() {
		List<Expression> expressions = asList(
			unresolvedCall(AS, unresolvedRef("field"), valueLiteral("fieldAlias")),
			new FieldReferenceExpression("resolvedField", DataTypes.INT(), 0, 0));
		resolverRule.apply(expressions, resolutionContext);
	}

	@Test(expected = TableException.class)
	public void testFlattenCallIsCatched() {
		List<Expression> expressions = singletonList(
			unresolvedCall(FLATTEN, new FieldReferenceExpression("resolvedField", DataTypes.INT(), 0, 0))
		);
		resolverRule.apply(expressions, resolutionContext);
	}

	@Test(expected = TableException.class)
	public void testUnresolvedOverWindowIsCatched() {
		List<Expression> expressions = singletonList(
			unresolvedCall(OVER,
				unresolvedCall(COUNT, new FieldReferenceExpression("resolvedField", DataTypes.INT(), 0, 0)),
				unresolvedRef("w")
			)
		);
		resolverRule.apply(expressions, resolutionContext);
	}

	@Test(expected = TableException.class)
	public void testLookupCallIsCatched() {
		List<Expression> expressions = singletonList(
			lookupCall("unresolvedCall", new FieldReferenceExpression("resolvedField", DataTypes.INT(), 0, 0))
		);
		resolverRule.apply(expressions, resolutionContext);
	}
}
