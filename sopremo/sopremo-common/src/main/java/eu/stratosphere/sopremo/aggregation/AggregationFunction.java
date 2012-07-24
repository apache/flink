package eu.stratosphere.sopremo.aggregation;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class AggregationFunction implements ISerializableSopremoType, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5701471344038419637L;

	private final String name;

	/**
	 * Initializes a new AggregationFunction with the given name.
	 * 
	 * @param name
	 *        the name of the function
	 */
	public AggregationFunction(final String name) {
		this.name = name;
	}

	/**
	 * This method must be implemented to define how this function aggregates the nodes
	 * 
	 * @param node
	 *        the node that should be added to the aggregate
	 * @param context
	 *        additional context informations
	 */
	public abstract IJsonNode aggregate(IJsonNode node, IJsonNode aggregator, EvaluationContext context);

	/**
	 * Creates an {@link AggregationExpression} for this function
	 * 
	 * @return the AggregationExpression
	 */
	public AggregationExpression asExpression() {
		return new AggregationExpression(this);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.name == null ? 0 : this.name.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final AggregationFunction other = (AggregationFunction) obj;
		if (this.name == null) {
			if (other.name != null)
				return false;
		} else if (!this.name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public AggregationFunction clone() {
		try {
			return (AggregationFunction) super.clone();
		} catch (final CloneNotSupportedException e) {
			throw new IllegalStateException("should not happen", e);
		}
	}

	/**
	 * Returns the result of all aggregation steps called before
	 * 
	 * @return the result
	 */
	public abstract IJsonNode getFinalAggregate(IJsonNode aggregator, IJsonNode target);

	/**
	 * Initializes this function
	 */
	public IJsonNode initialize(final IJsonNode aggregator) {
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
