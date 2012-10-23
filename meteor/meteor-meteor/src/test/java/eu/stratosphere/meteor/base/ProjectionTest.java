/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.meteor.base;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorTest;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * @author Arvid Heise
 */
public class ProjectionTest extends MeteorTest {

	@Test
	public void testProjection1() {
		final SopremoPlan actualPlan = this.parseScript("$input = read from 'input.json';\n" +
			"$result = transform $input into {sum: $input.a + $input.b};\n" +
			"write $result to 'output.json'; ");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("input.json");
		final Projection projection = new Projection().
			withResultProjection(
				new ObjectCreation(
					new ObjectCreation.FieldAssignment("sum",
						new ArithmeticExpression(JsonUtil.createPath("0", "a"), ArithmeticOperator.ADDITION,
							JsonUtil.createPath("0", "b"))))).
			withInputs(input);
		final Sink output = new Sink("output.json").withInputs(projection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

}
