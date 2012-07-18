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
package eu.stratosphere.sopremo.sdaa11;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Takes arrays as input and outputs them in sorted order according to the
 * nodes' natural order.
 * 
 * @author skruse
 * 
 */
public class Sort extends ElementaryOperator<Sort> {

	private static final long serialVersionUID = -5629400557338381919L;

	public static class Implementation extends SopremoMap {

		private final List<IJsonNode> nodes = new ArrayList<IJsonNode>();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo
		 * .type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			final IArrayNode inputNodes = (IArrayNode) value;
			this.nodes.clear();
			for (final IJsonNode inputNode : inputNodes)
				this.nodes.add(inputNode);
			Collections.sort(this.nodes);
			inputNodes.clear();
			inputNodes.addAll(this.nodes);
			out.collect(inputNodes);
		}

	}

}
