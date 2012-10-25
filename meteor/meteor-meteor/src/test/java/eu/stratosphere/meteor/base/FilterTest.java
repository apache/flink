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
package eu.stratosphere.meteor.base;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorTest;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.OrExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * @author Arvid Heise
 */
public class FilterTest extends MeteorTest {

	@Test
	public void testFilter() {
		final SopremoPlan actualPlan = this.parseScript("$input = read from 'input.json';\n" +
			"$result = filter $emp in $input where $emp.mgr or $emp.income > 30000;\n" +
			"write $result to 'output.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("input.json");
		final Selection selection = new Selection().
			withCondition(
				new OrExpression(
					new UnaryExpression(JsonUtil.createPath("0", "mgr")),
					new ComparativeExpression(JsonUtil.createPath("0", "income"), BinaryOperator.GREATER,
						new ConstantExpression(30000)))).
			withInputs(input);
		final Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		// System.out.println(Sop);

		assertEquals(expectedPlan, actualPlan);
	}

}
