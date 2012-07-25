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
package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.function.TransitiveAggregationFunction;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

final class ArrayUnion extends TransitiveAggregationFunction {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5358556436487835033L;

	public ArrayUnion() {
		super("U<values>", new ArrayNode());
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.aggregation.TransitiveAggregationFunction#aggregate(eu.stratosphere.sopremo.type.IJsonNode
	 * , eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public IJsonNode aggregate(IJsonNode node, IJsonNode aggregationTarget, EvaluationContext context) {
		IArrayNode array = (IArrayNode) node, unifiedArray = (IArrayNode) aggregationTarget;
		for (int index = 0; index < array.size(); index++)
			if (unifiedArray.get(index).isMissing() && !array.get(index).isMissing())
				unifiedArray.set(index, array.get(index));
		return unifiedArray;
	}
}