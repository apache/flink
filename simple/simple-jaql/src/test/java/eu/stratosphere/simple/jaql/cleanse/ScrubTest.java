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
package eu.stratosphere.simple.jaql.cleanse;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.OrExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public class ScrubTest extends SimpleTest {

	@Test
	public void testScrub() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
			"$dirty_earmarks = read hdfs('UsEarmark.json');\n" +
			"$nick_names = read hdfs('UsNickNames.json');\n" +
			"NormalizeName = javaudf('eu.stratosphere.simple.jaql.cleanse.ScrubTest.normalizeName');\n" +
			"$scrubbed_earmarks = scrub $dirty_earmark in $dirty_earmarks with {\n" +
			"	// normalization with built-in expressions\n" +
			"	amount: [$ as decimal, $ * 1000],\n" +
			"	// normalization with user-defined functions\n" +
			"	sponsorLastName: [required if $dirty_earmark.type == 's', NormalizeName],\n" +
			"	sponsorFirstName: [required, NormalizeName, replace $ with $nick_names default $],\n" +
			"};\n" +
			"write $scrubbed_earmarks to hdfs('scrubbed_earmarks.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source input = new Source("input.json");
		Selection selection = new Selection().
			withCondition(
				new OrExpression(
					new UnaryExpression(JsonUtil.createPath("$", "mgr")),
					new ComparativeExpression(JsonUtil.createPath("$", "income"), BinaryOperator.GREATER,
						new ConstantExpression(30000)))).
			withInputs(input);
		Sink output = new Sink("output.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	public static TextNode normalizeName(TextNode node) {
		return node;
	}
}
