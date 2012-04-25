package eu.stratosphere.sopremo.aggregation;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class TransitiveAggregationFunction extends AggregationFunction {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4836890030948315219L;

	private transient IJsonNode aggregate;

	private final IJsonNode initialAggregate;

	public TransitiveAggregationFunction(final String name, final IJsonNode initialAggregate) {
		super(name);
		this.initialAggregate = initialAggregate;
	}

	@Override
	public void aggregate(final IJsonNode node, final EvaluationContext context) {
		this.aggregate = this.aggregate(this.aggregate, node, context);
	}

	protected abstract IJsonNode aggregate(IJsonNode aggregate, IJsonNode node, EvaluationContext context);

	@Override
	public IJsonNode getFinalAggregate() {
		return this.aggregate;
	}

	@Override
	public void initialize() {
		this.aggregate = this.initialAggregate.clone();
	}
}
