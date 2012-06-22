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
package eu.stratosphere.sopremo.sdaa11.frequent_itemsets.dap;

import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.DegreeOfParallelism;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.dap.json.FrequentItemsetListNodes;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.dap.json.FrequentItemsetNodes;
import eu.stratosphere.sopremo.sdaa11.json.AnnotatorNodes;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author skruse
 * 
 */
@DegreeOfParallelism(value = 1)
@InputCardinality(value = 1)
@OutputCardinality(value = 1)
public class GroupFrequentItemsets extends
		ElementaryOperator<GroupFrequentItemsets> {

	private static final long serialVersionUID = -1076375108516926327L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.ElementaryOperator#getKeyExpressions(int)
	 */
	@Override
	public List<? extends EvaluationExpression> getKeyExpressions(
			final int inputIndex) {
		switch (inputIndex) {
		case 0:
			return Arrays.asList(new ObjectAccess(
					AnnotatorNodes.FLAT_ANNOTATION));
		default:
			throw new IllegalArgumentException("Illegal input index: "
					+ inputIndex);
		}
	}

	public static class Implementation extends SopremoReduce {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere
		 * .sopremo.type.IArrayNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void reduce(final IArrayNode values, final JsonCollector out) {
			final IArrayNode fisArrayNode = new ArrayNode();
			for (final IJsonNode value : values) {
				final ArrayNode fisNode = new ArrayNode();
				fisNode.copyValueFrom(FrequentItemsetNodes
						.getItems((ObjectNode) value));
				fisArrayNode.add(fisNode);
			}
			final ObjectNode fisListNode = new ObjectNode();
			FrequentItemsetListNodes.write(fisListNode, fisArrayNode);
			out.collect(fisListNode);
		}

	}

}
