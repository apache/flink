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
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * TODO Takes mapped point stream and cluster representation stream as input and
 * forwards those points that correspond to one of the cluster representations.
 * 
 * @author skruse
 * 
 */
public class PointSelection extends ElementaryOperator<PointSelection> {

	/**
	 * 
	 */
	private static final List<ObjectAccess> KEY_EXPRESSIONS = Arrays.asList(
			new ObjectAccess("clusterId"), new ObjectAccess("id"));
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
	public Iterable<? extends EvaluationExpression> getKeyExpressions() {
		return KEY_EXPRESSIONS;
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
			out.collect(((ObjectNode) mappedPointNode).get("point"));
		}

	}

}
