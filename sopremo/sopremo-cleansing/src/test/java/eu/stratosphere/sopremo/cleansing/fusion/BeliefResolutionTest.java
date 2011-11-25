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
package eu.stratosphere.sopremo.cleansing.fusion;

import it.unimi.dsi.fastutil.objects.Object2DoubleMap;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.fusion.BeliefResolution.BeliefMassFunction;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public class BeliefResolutionTest {
	public class AbbrExpression extends EvaluationExpression {
		/**
		 * 
		 */
		private static final long serialVersionUID = -5154400835672538630L;

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.JsonNode,
		 * eu.stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			String value1 = ((TextNode) ((ArrayNode) node).get(0)).getTextValue();
			String value2 = ((TextNode) ((ArrayNode) node).get(1)).getTextValue();
			return BooleanNode.valueOf(value1.endsWith(".")
				&& value2.startsWith(value1.substring(0, value1.length() - 1)));
		}
	}

	@Test
	public void testMassCombination() {
		BeliefResolution beliefResolution = new BeliefResolution(new AbbrExpression());

		TextNode john = new TextNode("John"), j = new TextNode("J."), bill = new TextNode("Bill");
		BeliefMassFunction massFunction = beliefResolution.getFinalMassFunction(
			new JsonNode[] { john, j, bill },
			new double[] { 0.8, 0.7, 0.9 }, new FusionContext(new EvaluationContext()));

		Object2DoubleMap<JsonNode> valueMasses = massFunction.getValueMasses();
		Assert.assertEquals(0.52, valueMasses.getDouble(john), 0.01);
		Assert.assertEquals(0.09, valueMasses.getDouble(j), 0.01);
		Assert.assertEquals(0.35, valueMasses.getDouble(bill), 0.01);
	}

	@Test
	public void testBeliefResolution() {
		BeliefResolution beliefResolution = new BeliefResolution(new AbbrExpression());

		TextNode john = new TextNode("John"), j = new TextNode("J."), bill = new TextNode("Bill");
		JsonNode result = beliefResolution.fuse(
			new JsonNode[] { john, j, bill },
			new double[] { 0.8, 0.7, 0.9 }, new FusionContext(new EvaluationContext()));

		Assert.assertEquals(john, result);
	}
}
