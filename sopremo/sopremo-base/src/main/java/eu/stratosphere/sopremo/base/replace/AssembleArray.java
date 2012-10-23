/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.base.replace;

import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.type.AbstractNumericNode;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * @author Arvid Heise
 */
@InputCardinality(1)
public class AssembleArray extends ElementaryOperator<AssembleArray> {
	private static final long serialVersionUID = 7334161941683036846L;

	/**
	 * Initializes AssembleArray.
	 */
	public AssembleArray() {
		this.setKeyExpressions(0, new ArrayAccess(2));
	}

	public static class Implementation extends SopremoReduce {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.IArrayNode,
		 * eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void reduce(IStreamArrayNode values, JsonCollector out) {
			ArrayNode assembledArray = new ArrayNode();

			int replacedCount = 0;
			IJsonNode lastValue = null;
			for (IJsonNode value : values) {
				int index = ((AbstractNumericNode) ((IArrayNode) value).get(1)).getIntValue();
				IJsonNode element = ((IArrayNode) value).get(0);
				assembledArray.set(index, element);
				replacedCount++;
				lastValue = value;
			}

			// cannot be null by definition of reduce
			@SuppressWarnings("null")
			final IJsonNode originalArray = ((IArrayNode) lastValue).get(2);
			int originalSize = ((IArrayNode) originalArray).size();
			// check if all values replaced if not filter array
			if (replacedCount == originalSize)
				out.collect(JsonUtil.asArray(originalArray, assembledArray));
		}
	}
}