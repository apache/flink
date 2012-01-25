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
package eu.stratosphere.simple.jaql.base;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ObjectCreation;

/**
 * @author Arvid Heise
 */
public class ProjectionTest extends SimpleTest {

	@Test
	public void testProjection1() {
		final SopremoPlan actualPlan = this.parseScript("$input = read 'input.json';\n" +
			"$result = project $input into {sum: $.a + $.b};\n" +
			"write $result to 'output.json'; ");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("input.json");
		final Projection projection = new Projection().
			withValueTransformation(
				new ObjectCreation(
					new ObjectCreation.FieldAssignment("sum",
						new ArithmeticExpression(JsonUtil.createPath("$", "a"), ArithmeticOperator.ADDITION,
							JsonUtil.createPath("$", "b"))))).
			withInputs(input);
		final Sink output = new Sink("output.json").withInputs(projection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testProjection2() {
		final SopremoPlan actualPlan = this.parseScript("$input = read 'input.json';\n" +
			"$result = project $entry in $input into { sum: $entry.a + $entry.b };\n" +
			"write $result to 'output.json'; ");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("input.json");
		final Projection projection = new Projection().
			withValueTransformation(
				new ObjectCreation(
					new ObjectCreation.FieldAssignment("sum",
						new ArithmeticExpression(JsonUtil.createPath("$", "a"), ArithmeticOperator.ADDITION,
							JsonUtil.createPath("$", "b"))))).
			withInputs(input);
		final Sink output = new Sink("output.json").withInputs(projection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

}
