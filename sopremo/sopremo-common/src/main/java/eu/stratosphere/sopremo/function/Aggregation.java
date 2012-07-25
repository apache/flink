package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class Aggregation<ElementType extends IJsonNode, AggregatorType extends IJsonNode> implements ISerializableSopremoType, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5701471344038419637L;

	private final String name;

	public Aggregation(final String name) {
		this.name = name;
	}

	public abstract AggregatorType aggregate(ElementType node, AggregatorType aggregator, EvaluationContext context);

	public AggregationExpression asExpression() {
		return new AggregationExpression(this);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result +  this.name.hashCode();
		return result;
	}
	
	/**
	 * Returns the name.
	 * 
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final Aggregation<ElementType, AggregatorType> other = (Aggregation<ElementType, AggregatorType>) obj;
		return this.name.equals(other.name);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Aggregation<ElementType, AggregatorType> clone() {
		try {
			return (Aggregation<ElementType, AggregatorType>) super.clone();
		} catch (final CloneNotSupportedException e) {
			throw new IllegalStateException("should not happen", e);
		}
	}

	public abstract IJsonNode getFinalAggregate(AggregatorType aggregator, IJsonNode target);

	public AggregatorType initialize(AggregatorType aggregator) {
		return aggregator;
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.name);
	}
}
