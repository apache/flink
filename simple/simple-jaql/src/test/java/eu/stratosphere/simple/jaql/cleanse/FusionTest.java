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
import eu.stratosphere.sopremo.cleansing.fusion.BeliefResolution;
import eu.stratosphere.sopremo.cleansing.fusion.Fusion;
import eu.stratosphere.sopremo.expressions.ArrayProjection;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public class FusionTest extends SimpleTest {

	@Test
	public void testSimpleFusion() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$personCluster = read 'personCluster.json';\n" +
			"$fusedPersons = fuse $personCluster\n" +
			"	into { \n" +
			"		id: generateId('person'),\n" +
			"	};\n" +
			"write $fusedPersons to hdfs('fusedPersons.json');");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source personCluster = new Source("personCluster.json");
		final Fusion fusion = new Fusion().
			withInputs(personCluster);
		fusion.addFusionRule(new MethodCall("generateId", new ConstantExpression("person")), new ObjectAccess("id"));
		final Sink fusedPersons = new Sink("fusedPersons.json").withInputs(fusion);
		expectedPlan.setSinks(fusedPersons);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testConflictResolution() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$personCluster = read 'personCluster.json';\n" +
			"abbr = fn(prefix, str) startsWith(prefix, str);\n" +
			"$fusedPersons = fuse $personCluster\n" +
			"	into { \n" +
			"		lastName: vote(abbr),\n" +
			"	};\n" +
			"write $fusedPersons to hdfs('fusedPersons.json');");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source personCluster = new Source("personCluster.json");
		final Fusion fusion = new Fusion().
			withInputs(personCluster);
		fusion.addFusionRule(new BeliefResolution(new MethodCall("abbr", EvaluationExpression.VALUE)),
			new ObjectAccess("lastName"));
		final Sink fusedPersons = new Sink("fusedPersons.json").withInputs(fusion);
		expectedPlan.setSinks(fusedPersons);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testRuleArray() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$personCluster = read 'personCluster.json';\n" +
			"abbr = fn(prefix, str) startsWith(prefix, str);\n" +
			"$fusedPersons = fuse $personCluster\n" +
			"	into { \n" +
			"		lastName: [vote(abbr), longest],\n" +
			"	};\n" +
			"write $fusedPersons to hdfs('fusedPersons.json');");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source personCluster = new Source("personCluster.json");
		final Fusion fusion = new Fusion().
			withInputs(personCluster);
		fusion.addFusionRule(new BeliefResolution(new MethodCall("abbr", EvaluationExpression.VALUE)),
			new ObjectAccess("lastName"));
		fusion.addFusionRule(new MethodCall("longest", EvaluationExpression.VALUE),
			new ObjectAccess("lastName"));
		final Sink fusedPersons = new Sink("fusedPersons.json").withInputs(fusion);
		expectedPlan.setSinks(fusedPersons);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testComplexInto() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
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

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source personCluster = new Source("personCluster.json");
		final Fusion fusion = new Fusion().
			withInputs(personCluster);
		fusion.addFusionRule(new MethodCall("generateId", new ConstantExpression("person")),
			new ObjectAccess("id"));
		fusion.addFusionRule(new BeliefResolution(new MethodCall("abbr", EvaluationExpression.VALUE)),
			new ObjectAccess("lastName"));
		fusion.addFusionRule(new MethodCall("longest", EvaluationExpression.VALUE),
			new ObjectAccess("lastName"));
		fusion.addFusionRule(new BeliefResolution(new MethodCall("abbr", EvaluationExpression.VALUE)),
			new ObjectAccess("firstName"));
		fusion.addFusionRule(new MethodCall("first", EvaluationExpression.VALUE),
			new ObjectAccess("firstName"));
		fusion.addFusionRule(new MethodCall("MergeAddresses", EvaluationExpression.VALUE),
			new ObjectAccess("addresses"));
		fusion.addFusionRule(new ArrayProjection(new ObjectAccess("id")),
			new ObjectAccess("originalRecords"));
		final Sink fusedPersons = new Sink("fusedPersons.json").withInputs(fusion);
		expectedPlan.setSinks(fusedPersons);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testDefault() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$personCluster = read 'personCluster.json';\n" +
			"abbr = fn(str, prefix) str + prefix;\n" +
			"MergeAddresses = javaudf('eu.stratosphere.simple.jaql.cleanse.FusionTest.mergeAddresses');\n" +
			"$fusedPersons = fuse $personCluster\n" +
			"	default vote(abbr);\n" +
			"write $fusedPersons to hdfs('fusedPersons.json');");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source personCluster = new Source("personCluster.json");
		final Fusion fusion = new Fusion().
			withInputs(personCluster);
		fusion.setDefaultValueRule(new BeliefResolution(new MethodCall("abbr", EvaluationExpression.VALUE)));
		final Sink fusedPersons = new Sink("fusedPersons.json").withInputs(fusion);
		expectedPlan.setSinks(fusedPersons);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testUpdate() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$personCluster = read 'personCluster.json';\n" +
			"abbr = fn(str, prefix) str + prefix;\n" +
			"MergeAddresses = javaudf('eu.stratosphere.simple.jaql.cleanse.FusionTest.mergeAddresses');\n" +
			"$fusedPersons = fuse [$member, $sponsor] in $personCluster\n" +
			"	update {\n" +
			"		$.relative[*].id: $.id,\n" +
			"	};\n" +
			"write $fusedPersons to hdfs('fusedPersons.json');");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source personCluster = new Source("personCluster.json");
		final Fusion fusion = new Fusion().
			withInputs(personCluster);
		fusion.addUpdateRule(JsonUtil.createPath("0", "id"),
			new PathExpression(new JsonStreamExpression(fusion), new ObjectAccess("relative"),
				new ArrayProjection(new ObjectAccess("id"))));

		final Sink fusedPersons = new Sink("fusedPersons.json").withInputs(fusion);
		expectedPlan.setSinks(fusedPersons);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testWeight() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$personCluster = read 'personCluster.json';\n" +
			"abbr = fn(str, prefix) str + prefix;\n" +
			"MergeAddresses = javaudf('eu.stratosphere.simple.jaql.cleanse.FusionTest.mergeAddresses');\n" +
			"$fusedPersons = fuse [$member, $sponsor] in $personCluster\n" +
			"	with weights { \n" +
			"		$member: 0.99,\n" +
			"		$sponsor: 0.99 * {\n" +
			"			addresses: 0.7\n" +
			"		}\n" +
			"	};\n" +
			"write $fusedPersons to hdfs('fusedPersons.json');");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source personCluster = new Source("personCluster.json");
		final Fusion fusion = new Fusion().
			withInputs(personCluster);
		fusion.setWeight(0.99, 0, new PathExpression());
		fusion.setWeight(0.99, 1, new PathExpression());
		fusion.setWeight(0.7, 1, new PathExpression(new ObjectAccess("addresses")));
		final Sink fusedPersons = new Sink("fusedPersons.json").withInputs(fusion);
		expectedPlan.setSinks(fusedPersons);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testComplexFusion() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$personCluster = read 'personCluster.json';\n" +
			"abbr = fn(str, prefix) str + prefix;\n" +
			"MergeAddresses = javaudf('eu.stratosphere.simple.jaql.cleanse.FusionTest.mergeAddresses');\n" +
			"$fusedPersons = fuse [$member, $sponsor] in $personCluster\n" +
			"	with weights { \n" +
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
			"	}\n" +
			"	update {\n" +
			"		$.relative[*].id: $.id,\n" +
			"	};\n" +
			"write $fusedPersons to hdfs('fusedPersons.json');");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source personCluster = new Source("personCluster.json");
		final Fusion fusion = new Fusion().
			withInputs(personCluster);
		fusion.addFusionRule(new MethodCall("generateId", new ConstantExpression("person")),
			new ObjectAccess("id"));
		fusion.addFusionRule(new BeliefResolution(new MethodCall("abbr", EvaluationExpression.VALUE)),
			new ObjectAccess("lastName"));
		fusion.addFusionRule(new MethodCall("longest", EvaluationExpression.VALUE),
			new ObjectAccess("lastName"));
		fusion.addFusionRule(new BeliefResolution(new MethodCall("abbr", EvaluationExpression.VALUE)),
			new ObjectAccess("firstName"));
		fusion.addFusionRule(new MethodCall("first", EvaluationExpression.VALUE),
			new ObjectAccess("firstName"));
		fusion.addFusionRule(new MethodCall("MergeAddresses", EvaluationExpression.VALUE),
			new ObjectAccess("addresses"));
		fusion.addFusionRule(new ArrayProjection(new ObjectAccess("id")), new ObjectAccess("originalRecords"));

		fusion.addUpdateRule(JsonUtil.createPath("0", "id"),
			new PathExpression(new JsonStreamExpression(fusion), new ObjectAccess("relative"),
				new ArrayProjection(new ObjectAccess("id"))));

		fusion.setWeight(0.99, 0, new PathExpression());
		fusion.setWeight(0.99, 1, new PathExpression());
		fusion.setWeight(0.7, 1, new PathExpression(new ObjectAccess("addresses")));
		final Sink fusedPersons = new Sink("fusedPersons.json").withInputs(fusion);
		expectedPlan.setSinks(fusedPersons);

		assertEquals(expectedPlan, actualPlan);
	}

	public static TextNode mergeAddresses(final TextNode node) {
		return node;
	}
}
