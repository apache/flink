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
public class FusionTest extends SimpleTest {

	@Test
	public void testFusion() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
			"$personCluster = read 'personCluster.json';\n" +
			"abbr = fn(str, prefix) str + prefix;\n" +
			"MergeAddresses = javaudf('eu.stratosphere.simple.jaql.cleanse.FusionTest.mergeAddresses');\n" +
			"$fusedPersons = fuse $personCluster\n" +
			"	into { \n" +
			"		id: generateId('person'),\n" +
			"		lastName: [vote(abbr), longest],\n" +
			"		firstName: [vote(abbr), first],\n" +
			"		addresses: MergeAddresses,\n" +
			"		originalRecords: $[*].id\n" +
			"	};\n" +
			"write $fusedPersons to hdfs('fusedPersons.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source input = new Source("input.json");
		Selection selection = new Selection().
			withCondition(
				new OrExpression(
					new UnaryExpression(JsonUtil.createPath("$", "mgr")),
					new ComparativeExpression(JsonUtil.createPath("$", "income"), BinaryOperator.GREATER,
						new ConstantExpression(30000)))).
			withInputs(input);
		Sink output = new Sink("fusedPersons.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testFusion2() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
			"$personCluster = read 'personCluster.json';\n" +
			"abbr = fn(str, prefix) str + prefix;\n" +
			"MergeAddresses = javaudf('eu.stratosphere.simple.jaql.cleanse.FusionTest.mergeAddresses');\n" +
			"$fusedPersons = fuse [$member, $sponsor] in $personCluster\n" +
			"	/*with weights { \n" +
			"		$member: 0.99,\n" +
			"		$sponsor: 0.99 * {\n" +
			"			addresses: 0.7\n" +
			"		}\n" +
			"	}\n" +
			"	into { \n" +
			"		id: generateId('person'),\n" +
			"		lastName: [vote(abbr), longest],\n" +
			"		firstName: [vote(abbr), first],\n" +
			"		addresses: MergeAddresses,\n" +
			"		originalRecords: $[*].id\n" +
			"	}*/\n" +
			"	update {\n" +
			"		$.relative[*].id: $.id,\n" +
			"	};\n" +
			"write $fusedPersons to hdfs('fusedPersons.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source input = new Source("input.json");
		Selection selection = new Selection().
			withCondition(
				new OrExpression(
					new UnaryExpression(JsonUtil.createPath("$", "mgr")),
					new ComparativeExpression(JsonUtil.createPath("$", "income"), BinaryOperator.GREATER,
						new ConstantExpression(30000)))).
			withInputs(input);
		Sink output = new Sink("fusedPersons.json").withInputs(selection);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	public static TextNode mergeAddresses(TextNode node) {
		return node;
	}
}
