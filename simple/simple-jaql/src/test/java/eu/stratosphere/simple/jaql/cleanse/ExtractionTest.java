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

/**
 * @author Arvid Heise
 */
public class ExtractionTest extends SimpleTest {

	@Test
	public void testExtraction() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
				"$scrubbed_earmarks = read 'scrubbedEarmarks.json';\n" +
				"extract from $scrubbed_earmarks into {\n" +
				"	$funds = group $ by $.earmarkId into {\n" +
				"		id: generateId('earmark'),\n" +
				"		amount: sum($[*].amount),\n" +
				"		currency: 'USD',\n" +
				"		date: {\n" +
				"			year: $[0].enactedYear\n" +
				"		},\n" +
				"		subject: $[0].shortDescription\n" +
				"	},\n" +
				"	$recipients = group $ by $.recipient into {\n" +
				"		/*id: generateId('earmark_person'),\n" +
				"		names: [$.recipient],*/\n" +
				"		receivedFunds: project $ into {\n" +
				"			id: $funds[$.earmarkId].id,\n" +
				"			amount: $.amount\n" +
				"		},\n" +
				"		category: $[0].recipientType\n" +
				"	}\n" +
				"};\n" +
				"write $funds to hdfs('Earmark_Funds.json');");

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

}
