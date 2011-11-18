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

import static eu.stratosphere.sopremo.JsonUtil.createPath;

import org.junit.Test;

import eu.stratosphere.simple.jaql.SimpleTest;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.OrExpression;

/**
 * @author Arvid Heise
 */
public class FinalJoinTest extends SimpleTest {

	@Test
	public void testComplexJoin() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
			"$persons = read 'persons.json';\n" +
			"$funds = read 'funds.json';\n" +
			"$legal_entity = read 'legal_entity.json';\n" +
			"\n" +
			"$result = join\n" +
			"	$sponsor in $persons,\n" +
			"	$relative in $persons,\n" +
			"	$fund in $funds,\n" +
			"	$recipient in $legal_entity,\n" +
			"	$subsidiary in $legal_entity\n" +
			"	where\n" +
			"	($relative.id in $sponsor.relatives[*].id or\n" +
			"	 $relative.id == $sponsor.id) and\n" +
			"	 $fund.id in $sponsor.enacted_funds[*].id and\n" +
			"	 $recipient.id in $fund.recipients and\n" +
			"	($subsidiary.id in $recipient.subsidiaries[*].id\n" +
			"	or $subsidiary.id == $recipient.id) and\n" +
			"	 $subsidiary.id in $relative.worksFor\n" +
			"	into {\n" +
			"	$sponsor.*,\n" +
			"	$relative.*\n" +
			"};\n" +
			"write $result to hdfs('result.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source persons = new Source("persons.json");
		Source funds = new Source("funds.json");
		Source legal_entity = new Source("legal_entity.json");
		Join join = new Join().
			withInputs(persons, persons, funds, legal_entity, legal_entity).
			withJoinCondition(
				new AndExpression(
					new OrExpression(
						new ElementInSetExpression(createPath("1", "id"), Quantor.EXISTS_IN, createPath("0", "relatives", "[*]", "id")),
						new ComparativeExpression(createPath("1", "id"), BinaryOperator.EQUAL, createPath("0", "id"))),
					new ElementInSetExpression(createPath("2", "id"), Quantor.EXISTS_IN, createPath("0", "enacted_funds", "[*]", "id")),
					new ElementInSetExpression(createPath("3", "id"), Quantor.EXISTS_IN, createPath("2", "recipients")),
					new OrExpression(
						new ElementInSetExpression(createPath("4", "id"), Quantor.EXISTS_IN, createPath("3", "subsidiaries", "[*]", "id")),
						new ComparativeExpression(createPath("4", "id"), BinaryOperator.EQUAL, createPath("3", "id"))),
					new ElementInSetExpression(createPath("4", "id"), Quantor.EXISTS_IN, createPath("1", "worksFor")))).
			withResultProjection(new ObjectCreation(
				new ObjectCreation.CopyFields(JsonUtil.createPath("0")),
				new ObjectCreation.CopyFields(JsonUtil.createPath("1"))
				));
		Sink result = new Sink("result.json").withInputs(join);
		expectedPlan.setSinks(result);

		assertEquals(expectedPlan, actualPlan);
	}
}
