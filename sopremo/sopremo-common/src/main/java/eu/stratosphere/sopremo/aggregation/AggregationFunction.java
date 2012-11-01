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
import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;

/**
 * @author Arvid Heise
 */
public class AggregationFunction extends SopremoFunction {
	/**
	 * 
	 */
	private static final long serialVersionUID = 844772928287791987L;

	private final Aggregation<IJsonNode, IJsonNode> aggregation;

	/**
	 * Initializes AggregationFunction.
	 * 
	 * @param aggregation
	 */
	@SuppressWarnings("unchecked")
	public AggregationFunction(Aggregation<?, ?> aggregation) {
		if (aggregation == null)
			throw new NullPointerException();
		this.aggregation = (Aggregation<IJsonNode, IJsonNode>) aggregation;
	}

	/**
	 * Returns the aggregation.
	 * 
	 * @return the aggregation
	 */
	public Aggregation<IJsonNode, IJsonNode> getAggregation() {
		return this.aggregation;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object, java.lang.Object,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public IJsonNode call(IArrayNode params, IJsonNode target, EvaluationContext context) {
		checkParameters(params, 1);

		IJsonNode aggregator = this.aggregation.initialize(target);

		for (IJsonNode item : (IStreamArrayNode) params.get(0))
			aggregator = this.aggregation.aggregate(item, aggregator, context);

		return target = this.aggregation.getFinalAggregate(aggregator, target);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.aggregation.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AggregationFunction other = (AggregationFunction) obj;
		return this.aggregation.equals(other.aggregation);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		this.aggregation.toString(builder);
	}
}
