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
package eu.stratosphere.sopremo.sdaa11.clustering.postprocessing;

import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.sdaa11.clustering.json.PointNodes;
import eu.stratosphere.sopremo.sdaa11.clustering.json.RepresentationNodes;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * TODO Takes mapped point stream and cluster representation stream as input and
 * forwards those points that correspond to one of the cluster representations.
 * input1: points input2: representations
 * 
 * @author skruse
 * 
 */
@InputCardinality(min = 2, max = 2)
public class PointSelection extends ElementaryOperator<PointSelection> {

	/**
	 * 
	 */
	private static final List<ObjectAccess> POINT_KEY_EXPRESSIONS = Arrays
			.asList(new ObjectAccess(PointNodes.CLUSTER_ID));

	private static final List<ObjectAccess> REPRESENTATION_KEY_EXPRESSION = Arrays
			.asList(new ObjectAccess(RepresentationNodes.PARENT_ID));

	/**
	 * 
	 */
	private static final long serialVersionUID = -9099806693649666122L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.ElementaryOperator#getKeyExpressions()
	 */
	@Override
	public List<? extends EvaluationExpression> getKeyExpressions(
			final int inputIndex) {
		switch (inputIndex) {
		case 0:
			return POINT_KEY_EXPRESSIONS;
		case 1:
			return REPRESENTATION_KEY_EXPRESSION;
		}
		throw new IllegalArgumentException("Illegal input index: " + inputIndex);
	}

	public static class Implementation extends SopremoMatch {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoMatch#match(eu.stratosphere.sopremo
		 * .type.IJsonNode, eu.stratosphere.sopremo.type.IJsonNode,
		 * eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void match(final IJsonNode mappedPointNode,
				final IJsonNode clusterRepresentationNode,
				final JsonCollector out) {
			((ObjectNode) mappedPointNode).remove("clusterId");
			out.collect(mappedPointNode);
		}

	}

}
