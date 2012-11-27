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
package eu.stratosphere.sopremo.aggregation;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public class ArrayAccessAsAggregation extends Aggregation<IJsonNode, ArrayNode> {
	private int startIndex, endIndex, elementsToSkip, remainingElements;

	private boolean range;

	/**
	 * Initializes ArrayAccessAsAggregation.
	 * 
	 * @param name
	 */
	public ArrayAccessAsAggregation(int startIndex, int endIndex, boolean range) {
		super("Array access");
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.range = range;
	}

	public ArrayAccessAsAggregation(int index) {
		this(index, index, false);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -3359162289544753192L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.aggregation.Aggregation#initialize(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public ArrayNode initialize(ArrayNode aggregator) {
		this.elementsToSkip = this.startIndex;
		this.remainingElements = this.endIndex - this.startIndex + 1;
		return SopremoUtil.reinitializeTarget(aggregator, ArrayNode.class);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.aggregation.Aggregation#aggregate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public ArrayNode aggregate(IJsonNode node, ArrayNode aggregator, EvaluationContext context) {
		if (this.elementsToSkip > 0)
			this.elementsToSkip--;
		else if (this.remainingElements > 0) {
			aggregator.add(node);
			this.remainingElements--;
		}
		return aggregator;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.aggregation.Aggregation#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		super.toString(builder);
		builder.append('[');
		builder.append(this.startIndex);
		if (this.startIndex != this.endIndex) {
			builder.append(':');
			builder.append(this.endIndex);
		}
		builder.append(']');
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.aggregation.Aggregation#getFinalAggregate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode getFinalAggregate(ArrayNode aggregator, IJsonNode target) {
		if (this.range) {
			final ArrayNode targetArray = SopremoUtil.reinitializeTarget(target, ArrayNode.class);
			targetArray.copyValueFrom(aggregator);
		}
		final IJsonNode targetNode = SopremoUtil.ensureType(target, aggregator.get(0).getClass());
		targetNode.copyValueFrom(aggregator.get(0));
		return targetNode;
	}

}
