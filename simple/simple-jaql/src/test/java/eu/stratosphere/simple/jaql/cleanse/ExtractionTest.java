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
import eu.stratosphere.simple.jaql.StreamIndexExpression;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.cleansing.scrubbing.EntityExtraction;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ArrayProjection;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.expressions.NestedOperatorExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;

/**
 * @author Arvid Heise
 */
public class ExtractionTest extends SimpleTest {

	@Test
	public void testTrivialExtraction() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$scrubbedEarmarks = read 'scrubbedEarmarks.json';\n" +
			"extract from $scrubbedEarmarks into {\n" +
			"};\n" +
			"write $scrubbedEarmarks to hdfs('Earmark.json');\n");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source scrubbedEarmarks = new Source("scrubbedEarmarks.json");
		final Sink funds = new Sink("Earmark.json").withInputs(scrubbedEarmarks);
		expectedPlan.setSinks(funds);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testOneSourceExtraction() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$scrubbedEarmarks = read 'scrubbedEarmarks.json';\n" +
			"extract from $scrubbedEarmarks into {\n" +
			"	$funds = group $ by $.earmarkId into {\n" +
			"		id: generateId('earmark'),\n" +
			"		amount: sum($[*].amount),\n" +
			"		currency: 'USD',\n" +
			"		date: {\n" +
			"			year: $[0].enactedYear\n" +
			"		},\n" +
			"		subject: $[0].shortDescription\n" +
			"	},\n" +
			"};\n" +
			"write $funds to hdfs('Earmark_Funds.json');\n");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source scrubbedEarmarks = new Source("scrubbedEarmarks.json");
		final EntityExtraction extraction = new EntityExtraction().
			withInputs(scrubbedEarmarks);
		Grouping grouping = new Grouping().
			withInputs(extraction.getInputs().get(0)).
			withGroupingKey(JsonUtil.createPath("0", "earmarkId")).
			withResultProjection(
				new ObjectCreation(
					new ObjectCreation.FieldAssignment("id",
						new MethodCall("generateId", new ConstantExpression("earmark"))),
					new ObjectCreation.FieldAssignment("amount",
						new MethodCall("sum", new PathExpression(new InputSelection(0), new ArrayProjection(
							new ObjectAccess("amount"))))),
					new ObjectCreation.FieldAssignment("currency", new ConstantExpression("USD")),

					new ObjectCreation.FieldAssignment("date",
						new ObjectCreation(
							new ObjectCreation.FieldAssignment("year", JsonUtil.createPath("0", "[0]", "enactedYear"))
						)),
					new ObjectCreation.FieldAssignment("subject", JsonUtil.createPath("0", "[0]", "shortDescription"))));
		extraction.addExtraction(grouping);
		final Sink funds = new Sink("Earmark_Funds.json").withInputs(extraction);
		expectedPlan.setSinks(funds);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testTwoSourceExtraction() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$scrubbedEarmarks = read 'scrubbedEarmarks.json';\n" +
			"extract from $scrubbedEarmarks into {\n" +
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
			"		id: generateId('earmark_person'),\n" +
			"		names: [$.recipient],\n" +
			"		category: $[0].recipientType\n" +
			"	}\n" +
			"};\n" +
			"write $recipients to hdfs('Earmark_Recipients.json');\n" +
			"write $funds to hdfs('Earmark_Funds.json');\n");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source scrubbedEarmarks = new Source("scrubbedEarmarks.json");
		final EntityExtraction extraction = new EntityExtraction().
			withInputs(scrubbedEarmarks);
		Grouping fundExtraction = new Grouping().
			withInputs(extraction.getInputs().get(0)).
			withGroupingKey(JsonUtil.createPath("0", "earmarkId")).
			withResultProjection(
				new ObjectCreation(
					new ObjectCreation.FieldAssignment("id",
						new MethodCall("generateId", new ConstantExpression("earmark"))),
					new ObjectCreation.FieldAssignment("amount",
						new MethodCall("sum", new PathExpression(new InputSelection(0), new ArrayProjection(
							new ObjectAccess("amount"))))),
					new ObjectCreation.FieldAssignment("currency", new ConstantExpression("USD")),

					new ObjectCreation.FieldAssignment("date",
						new ObjectCreation(
							new ObjectCreation.FieldAssignment("year", JsonUtil.createPath("0", "[0]", "enactedYear"))
						)),
					new ObjectCreation.FieldAssignment("subject", JsonUtil.createPath("0", "[0]", "shortDescription"))));

		Grouping recepientExtraction = new Grouping().
			withInputs(extraction.getInputs().get(0)).
			withGroupingKey(JsonUtil.createPath("0", "recipient")).
			withResultProjection(
				new ObjectCreation(
					new ObjectCreation.FieldAssignment("id",
						new MethodCall("generateId", new ConstantExpression("earmark_person"))),
					new ObjectCreation.FieldAssignment("names",
						new ArrayCreation(JsonUtil.createPath("0", "recipient"))),
					new ObjectCreation.FieldAssignment("category", JsonUtil.createPath("0", "[0]", "recipientType"))));

		extraction.addExtraction(fundExtraction);
		extraction.addExtraction(recepientExtraction);
			
		expectedPlan.setSinks(new Sink("Earmark_Recipients.json").withInputs(extraction.getOutput(1)),
			new Sink("Earmark_Funds.json").withInputs(extraction.getOutput(0)));

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testInlineOperator() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$scrubbedEarmarks = read 'scrubbedEarmarks.json';\n" +
			"extract from $scrubbedEarmarks into {\n" +
			"	$recipients = group $ by $.recipient into {\n" +
			"		id: generateId('earmark_person'),\n" +
			"		names: [$.recipient],\n" +
			"		receivedFunds: project $ into {\n" +
			"			amount: $.amount\n" +
			"		},\n" +
			"		category: $[0].recipientType\n" +
			"	}\n" +
			"};\n" +
			"write $recipients to hdfs('Earmark_Recipients.json');\n");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source scrubbedEarmarks = new Source("scrubbedEarmarks.json");
		final EntityExtraction extraction = new EntityExtraction().
			withInputs(scrubbedEarmarks);

		Grouping recepientExtraction = new Grouping();
		
		Projection inlineProjection = new Projection().
				withInputs(recepientExtraction).
				withValueTransformation(new ObjectCreation(
					new ObjectCreation.FieldAssignment("amount", JsonUtil.createPath("0", "amount"))
					));
		
		recepientExtraction.
				withInputs(extraction.getInputs().get(0)).
				withGroupingKey(JsonUtil.createPath("0", "recipient")).
				withResultProjection(
					new ObjectCreation(
						new ObjectCreation.FieldAssignment("id",
							new MethodCall("generateId", new ConstantExpression("earmark_person"))),
						new ObjectCreation.FieldAssignment("names",
							new ArrayCreation(JsonUtil.createPath("0", "recipient"))),
						new ObjectCreation.FieldAssignment("receivedFunds",
							new NestedOperatorExpression(inlineProjection)),
						new ObjectCreation.FieldAssignment("category", JsonUtil.createPath("0", "[0]", "recipientType"))));
	
		extraction.addExtraction(recepientExtraction);
			
		expectedPlan.setSinks(new Sink("Earmark_Recipients.json").withInputs(extraction.getOutput(1)));

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testSideLookup() {
		final SopremoPlan actualPlan = this.parseScript("using cleansing;\n" +
			"$scrubbedEarmarks = read 'scrubbedEarmarks.json';\n" +
			"extract from $scrubbedEarmarks into {\n" +
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
			"		id: generateId('earmark_person'),\n" +
			"		names: [$.recipient],\n" +
			"		receivedFunds: project $ into {\n" +
			"			id: $funds[$.earmarkId].id,\n" +
			"			amount: $.amount\n" +
			"		},\n" +
			"		category: $[0].recipientType\n" +
			"	}\n" +
			"};\n" +
			"write $funds to hdfs('Earmark_Funds.json');\n" +
			"write $recipients to hdfs('Earmark_Recipients.json');");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source scrubbedEarmarks = new Source("scrubbedEarmarks.json");
		final EntityExtraction extraction = new EntityExtraction().
			withInputs(scrubbedEarmarks);
		Grouping fundExtraction = new Grouping().
			withInputs(extraction.getInputs().get(0)).
			withGroupingKey(JsonUtil.createPath("0", "earmarkId")).
			withResultProjection(
				new ObjectCreation(
					new ObjectCreation.FieldAssignment("id",
						new MethodCall("generateId", new ConstantExpression("earmark"))),
					new ObjectCreation.FieldAssignment("amount",
						new MethodCall("sum", new PathExpression(new InputSelection(0), new ArrayProjection(
							new ObjectAccess("amount"))))),
					new ObjectCreation.FieldAssignment("currency", new ConstantExpression("USD")),

					new ObjectCreation.FieldAssignment("date",
						new ObjectCreation(
							new ObjectCreation.FieldAssignment("year", JsonUtil.createPath("0", "[0]", "enactedYear"))
						)),
					new ObjectCreation.FieldAssignment("subject", JsonUtil.createPath("0", "[0]", "shortDescription"))));

		Grouping recepientExtraction = new Grouping();
		Projection inlineProjection = new Projection().
			withInputs(recepientExtraction).
			withValueTransformation(new ObjectCreation(
				new ObjectCreation.FieldAssignment("id",
					new PathExpression(new StreamIndexExpression(extraction.getOutput(0), JsonUtil.createPath("0", "earmarkId")), new ObjectAccess("id"))),
				new ObjectCreation.FieldAssignment("amount", JsonUtil.createPath("0", "amount"))
				));
		
		recepientExtraction.
			withInputs(extraction.getInputs().get(0)).
			withGroupingKey(JsonUtil.createPath("0", "recipient")).
			withResultProjection(
				new ObjectCreation(
					new ObjectCreation.FieldAssignment("id",
						new MethodCall("generateId", new ConstantExpression("earmark_person"))),
					new ObjectCreation.FieldAssignment("names",
						new ArrayCreation(JsonUtil.createPath("0", "recipient"))),
					new ObjectCreation.FieldAssignment("receivedFunds",
						new NestedOperatorExpression(inlineProjection)),
					new ObjectCreation.FieldAssignment("category", JsonUtil.createPath("0", "[0]", "recipientType"))));
	
		extraction.addExtraction(fundExtraction);
		extraction.addExtraction(recepientExtraction);
			
		expectedPlan.setSinks(new Sink("Earmark_Recipients.json").withInputs(extraction.getOutput(1)),
			new Sink("Earmark_Funds.json").withInputs(extraction.getOutput(0)));

		assertEquals(expectedPlan, actualPlan);
	}

}
