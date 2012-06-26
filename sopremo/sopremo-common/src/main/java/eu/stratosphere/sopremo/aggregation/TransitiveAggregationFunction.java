package eu.stratosphere.sopremo.aggregation;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class TransitiveAggregationFunction extends AggregationFunction {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4836890030948315219L;

	private final IJsonNode initialAggregate;

	public TransitiveAggregationFunction(final String name, final IJsonNode initialAggregate) {
		super(name);
		this.initialAggregate = initialAggregate;
	}

	@Override
	public abstract IJsonNode aggregate(final IJsonNode node, final IJsonNode aggregationTarget,
			final EvaluationContext context);

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.aggregation.AggregationFunction#getFinalAggregate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode getFinalAggregate(IJsonNode aggregator, IJsonNode target) {
		return aggregator;
	}

	@Override
	public IJsonNode initialize(IJsonNode aggregationTarget) {
		aggregationTarget = SopremoUtil.reinitializeTarget(aggregationTarget, this.initialAggregate.getClass());
		aggregationTarget.copyValueFrom(aggregationTarget);
		return aggregationTarget;
	}
}
