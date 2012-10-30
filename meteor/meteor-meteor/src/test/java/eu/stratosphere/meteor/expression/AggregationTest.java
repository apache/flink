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
package eu.stratosphere.meteor.expression;

import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorTest;
import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.aggregation.ArrayAccessAsAggregation;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BatchAggregationExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpressionUtil;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoPlan;

/**
 * @author Arvid Heise
 */
public class AggregationTest extends MeteorTest {
	@Test
	public void test() {
		final SopremoPlan actualPlan =
			this.parseScript("$li = read from 'file:///lineitem.json';\n"				+
				"$filterLi = filter $li where $li.l_linenumber >= 1;\n" +
				"$groups = group $filterLi by [$filterLi.l_linestatus, $filterLi.l_returnflag] into {\n" +
				"     first: $filterLi[0],\n" +
				"	  count_qty: count($filterLi),\n" +
				"	  sum_qty: sum($filterLi[*].l_quantity),\n" +
				"	  mean_qty: mean($filterLi[*].l_quantity)\n" +
				"};\n" +
				"write $groups to 'file:///q1.result';\n");

		final Source input = new Source("file:///lineitem.json");
		final Selection filter = new Selection().
			withInputs(input).
			withCondition(new ComparativeExpression(new ObjectAccess("l_linenumber"), 
				BinaryOperator.GREATER_EQUAL, 
				new ConstantExpression(1)));	
		final BatchAggregationExpression batch = new BatchAggregationExpression();
		final Grouping grouping = new Grouping().
				withInputs(filter).
				withGroupingKey(0, new ArrayCreation(new ObjectAccess("l_linestatus"), new ObjectAccess("l_returnflag"))).
				withResultProjection(new ObjectCreation(
					new ObjectCreation.FieldAssignment("first", batch.add(new ArrayAccessAsAggregation(0))),
					new ObjectCreation.FieldAssignment("count_qty", batch.add(CoreFunctions.COUNT)),
					new ObjectCreation.FieldAssignment("sum_qty", batch.add(CoreFunctions.SUM, new ObjectAccess("l_quantity"))),
					new ObjectCreation.FieldAssignment("mean_qty", batch.add(CoreFunctions.MEAN, new ObjectAccess("l_quantity")))
				));

		final Sink sink = new Sink("file:///q1.result").withInputs(grouping);
		final SopremoPlan expectedPlan = new SopremoPlan();
		expectedPlan.setSinks(sink);
		SopremoTest.assertEquals(expectedPlan, actualPlan);
		
//		final Iterable<? extends Operator<?>> containedOperators = actualPlan.getContainedOperators();
//		final ArrayList<Operator> ops = new ArrayList<Operator>();
//		for (Operator<?> operator : containedOperators)
//			ops.add(operator);
//		final Grouping grouping = (Grouping) ops.get(1);
//		System.out.println(grouping.getResultProjection().printAsTree());
//		System.out.println();
//		System.out.println(EvaluationExpressionUtil.replaceAggregationWithBatchAggregation(
//			grouping.getResultProjection()).printAsTree());
	}
}
