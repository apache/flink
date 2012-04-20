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
package eu.stratosphere.simple.jaql.expression;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.type.JsonNode;

/**
 * @author Arvid Heise
 */
public class FunctionTest extends SimpleTest {

	@Test
	public void testFunctionDefinition() {
		final SopremoPlan actualPlan = this.parseScript("square = fn(x) x * x;\n" +
			"$input = read 'input.json';\n" +
			"$result = project $input into { squared: square($input) };\n" +
			"write $result to 'output.json'; ");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("input.json");
		final Projection projection = new Projection().
			withInputs(input).
			withValueTransformation(new ObjectCreation(
				new ObjectCreation.FieldAssignment("squared",
					new ArithmeticExpression(new InputSelection(0), ArithmeticOperator.MULTIPLICATION,
						new InputSelection(0)))));
		final Sink sink = new Sink("output.json").withInputs(projection);
		expectedPlan.setSinks(sink);

		Assert.assertEquals("unexpectedPlan", expectedPlan, actualPlan);
	}

	@Test
	public void testFunctionImport() {
		final SopremoPlan actualPlan = this
			.parseScript("testudf = javaudf('eu.stratosphere.simple.jaql.expression.FunctionTest.udfTest');\n" +
				"$input = read 'input.json';\n" +
				"$result = project $input into { squared: testudf($input) };\n" +
				"write $result to 'output.json'; ");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("input.json");
		final Projection projection = new Projection().
			withInputs(input).
			withValueTransformation(new ObjectCreation(
				new ObjectCreation.FieldAssignment("squared", new MethodCall("testudf", new InputSelection(0)))));
		final Sink sink = new Sink("output.json").withInputs(projection);
		expectedPlan.setSinks(sink);

		Assert.assertEquals("unexpectedPlan", expectedPlan, actualPlan);
	}

	public static JsonNode udfTest(final JsonNode... nodes) {
		return nodes[0];
	}
}
