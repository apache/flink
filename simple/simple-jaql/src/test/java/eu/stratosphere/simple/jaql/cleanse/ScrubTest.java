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
import eu.stratosphere.sopremo.base.Replace;
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.Scrubbing;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.CoerceExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.expressions.NestedOperatorExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.TernaryExpression;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public class ScrubTest extends SimpleTest {

	/**
	 * 
	 */
	private static final InputSelection CURRENT_VALUE = new InputSelection(0);

	@Test
	public void testSingleRule() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
			"$dirty_earmarks = read hdfs('UsEarmark.json');\n" +
			"$scrubbed_earmarks = scrub $dirty_earmark in $dirty_earmarks with {\n" +
			"	// normalization with built-in expressions\n" +
			"	amount: $ as decimal,\n" +
			"};\n" +
			"write $scrubbed_earmarks to hdfs('scrubbed_earmarks.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source dirty_earmarks = new Source("UsEarmark.json");
		Scrubbing scrubbing = new Scrubbing().
			withInputs(dirty_earmarks);
		scrubbing.addRawRule(new CoerceExpression(DecimalNode.class, CURRENT_VALUE), new ObjectAccess("amount"));
		Sink output = new Sink("scrubbed_earmarks.json").withInputs(scrubbing);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testDoubleRule() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
			"$dirty_earmarks = read hdfs('UsEarmark.json');\n" +
			"$scrubbed_earmarks = scrub $dirty_earmark in $dirty_earmarks with {\n" +
			"	// normalization with built-in expressions\n" +
			"	amount: [$ as decimal, $ * 1000],\n" +
			"};\n" +
			"write $scrubbed_earmarks to hdfs('scrubbed_earmarks.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source dirty_earmarks = new Source("UsEarmark.json");
		Scrubbing scrubbing = new Scrubbing().
			withInputs(dirty_earmarks);
		scrubbing.addRawRule(new CoerceExpression(DecimalNode.class, CURRENT_VALUE), new ObjectAccess("amount"));
		scrubbing.addRawRule(new ArithmeticExpression(CURRENT_VALUE, ArithmeticOperator.MULTIPLICATION,
			new ConstantExpression(1000)), new ObjectAccess("amount"));
		Sink output = new Sink("scrubbed_earmarks.json").withInputs(scrubbing);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testRequired() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
			"$dirty_earmarks = read hdfs('UsEarmark.json');\n" +
			"$scrubbed_earmarks = scrub $dirty_earmark in $dirty_earmarks with {\n" +
			"	sponsorLastName: required,\n" +
			"};\n" +
			"write $scrubbed_earmarks to hdfs('scrubbed_earmarks.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source dirty_earmarks = new Source("UsEarmark.json");
		Scrubbing scrubbing = new Scrubbing().
			withInputs(dirty_earmarks);
		scrubbing.addRule(new NonNullRule(), new ObjectAccess("sponsorLastName"));
		Sink output = new Sink("scrubbed_earmarks.json").withInputs(scrubbing);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testConditionalRequired() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
			"$dirty_earmarks = read hdfs('UsEarmark.json');\n" +
			"$scrubbed_earmarks = scrub $dirty_earmark in $dirty_earmarks with {\n" +
			"	sponsorLastName: required if $dirty_earmark.type == 's',\n" +
			"};\n" +
			"write $scrubbed_earmarks to hdfs('scrubbed_earmarks.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source dirty_earmarks = new Source("UsEarmark.json");
		Scrubbing scrubbing = new Scrubbing().
			withInputs(dirty_earmarks);
		scrubbing.addRawRule(new TernaryExpression(new ComparativeExpression(JsonUtil.createPath("1", "type"),
			BinaryOperator.EQUAL, new ConstantExpression("s")),
			new PathExpression(new InputSelection(0), new NonNullRule()),
			EvaluationExpression.VALUE), new ObjectAccess("sponsorLastName"));
		Sink output = new Sink("scrubbed_earmarks.json").withInputs(scrubbing);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testMethodCall() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
			"$dirty_earmarks = read hdfs('UsEarmark.json');\n" +
			"NormalizeName = javaudf('eu.stratosphere.simple.jaql.cleanse.ScrubTest.normalizeName');\n" +
			"$scrubbed_earmarks = scrub $dirty_earmark in $dirty_earmarks with {\n" +
			"	sponsorLastName: NormalizeName,\n" +
			"};\n" +
			"write $scrubbed_earmarks to hdfs('scrubbed_earmarks.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source dirty_earmarks = new Source("UsEarmark.json");
		Scrubbing scrubbing = new Scrubbing().
			withInputs(dirty_earmarks);
		scrubbing.addRawRule(new MethodCall("NormalizeName", new InputSelection(0)),
			new ObjectAccess("sponsorLastName"));
		Sink output = new Sink("scrubbed_earmarks.json").withInputs(scrubbing);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testInlineOperator() {
		SopremoPlan actualPlan = parseScript("using cleansing;\n" +
			"$dirty_earmarks = read hdfs('UsEarmark.json');\n" +
			"$nick_names = read hdfs('UsNickNames.json');\n" +
			"NormalizeName = javaudf('eu.stratosphere.simple.jaql.cleanse.ScrubTest.normalizeName');\n" +
			"$scrubbed_earmarks = scrub $dirty_earmark in $dirty_earmarks with {\n" +
			"	sponsorFirstName: replace $ with $nick_names default $,\n" +
			"};\n" +
			"write $scrubbed_earmarks to hdfs('scrubbed_earmarks.json');");

		SopremoPlan expectedPlan = new SopremoPlan();
		Source dirty_earmarks = new Source("UsEarmark.json");
		Source nick_names = new Source("UsNickNames.json");
		Scrubbing scrubbing = new Scrubbing().
			withInputs(dirty_earmarks);
		Replace replace = new Replace().
			withInputs(dirty_earmarks, nick_names).
			withDefaultExpression(new InputSelection(0).withTag(JsonStreamExpression.THIS_CONTEXT));
		scrubbing.addRawRule(new NestedOperatorExpression(replace), new ObjectAccess("sponsorFirstName"));
		Sink output = new Sink("scrubbed_earmarks.json").withInputs(scrubbing);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testBigScrub() {
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
		Source dirty_earmarks = new Source("UsEarmark.json");
		Source nick_names = new Source("UsNickNames.json");
		Scrubbing scrubbing = new Scrubbing().
			withInputs(dirty_earmarks);
		scrubbing.addRawRule(new CoerceExpression(DecimalNode.class, CURRENT_VALUE), new ObjectAccess("amount"));
		scrubbing.addRawRule(new ArithmeticExpression(CURRENT_VALUE, ArithmeticOperator.MULTIPLICATION,
			new ConstantExpression(1000)), new ObjectAccess("amount"));

		scrubbing.addRawRule(new TernaryExpression(new ComparativeExpression(JsonUtil.createPath("1", "type"),
			BinaryOperator.EQUAL, new ConstantExpression("s")),
			new PathExpression(new InputSelection(0), new NonNullRule()),
			EvaluationExpression.VALUE), new ObjectAccess("sponsorLastName"));
		scrubbing.addRawRule(new MethodCall("NormalizeName", new InputSelection(0)),
			new ObjectAccess("sponsorLastName"));

		scrubbing.addRule(new NonNullRule(), new ObjectAccess("sponsorFirstName"));
		scrubbing.addRawRule(new MethodCall("NormalizeName", new InputSelection(0)),
			new ObjectAccess("sponsorFirstName"));
		Replace replace = new Replace().
			withInputs(dirty_earmarks, nick_names).
			withDefaultExpression(new InputSelection(0).withTag(JsonStreamExpression.THIS_CONTEXT));
		scrubbing.addRawRule(new NestedOperatorExpression(replace), new ObjectAccess("sponsorFirstName"));

		Sink output = new Sink("scrubbed_earmarks.json").withInputs(scrubbing);
		expectedPlan.setSinks(output);

		assertEquals(expectedPlan, actualPlan);
	}

	public static TextNode normalizeName(TextNode node) {
		return node;
	}
}
