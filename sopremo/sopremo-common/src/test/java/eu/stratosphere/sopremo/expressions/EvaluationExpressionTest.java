/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;

/**
 * @author Arvid Heise
 */
public class EvaluationExpressionTest {
	@Test
	public void shouldFindValues() {
		final InputSelection inputSelection = new InputSelection(1);
		final ObjectAccess objectAccess = new ObjectAccess("test");
		final PathExpression pathExpression = new PathExpression(inputSelection, objectAccess);
		final EvaluationExpression expression =
			new ArithmeticExpression(pathExpression, ArithmeticOperator.ADDITION, ConstantExpression.MISSING);

		Assert.assertSame(inputSelection, expression.find(InputSelection.class));
		Assert.assertSame(objectAccess, expression.find(ObjectAccess.class));
	}

	@Test
	public void shouldRemoveValues() {
		final InputSelection inputSelection = new InputSelection(1);
		final ObjectAccess objectAccess = new ObjectAccess("test");
		final PathExpression pathExpression = new PathExpression(inputSelection, objectAccess);
		final EvaluationExpression expression =
			new ArithmeticExpression(pathExpression, ArithmeticOperator.ADDITION, ConstantExpression.MISSING);
		final EvaluationExpression referenceExpression = expression.clone();

		Assert.assertEquals(expression, referenceExpression);

		// removed not the outer expression
		Assert.assertSame(expression, expression.remove(ObjectAccess.class));

		Assert.assertTrue(expression.equals(referenceExpression));

		final EvaluationExpression expected =
			new ArithmeticExpression(new PathExpression(inputSelection), ArithmeticOperator.ADDITION,
				ConstantExpression.MISSING);

		Assert.assertEquals(expression, expected);
		Assert.assertSame(inputSelection, expression.find(InputSelection.class));
	}
}
