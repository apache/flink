package eu.stratosphere.sopremo.aggregation;

import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class TransitiveAggregation<ElementType extends IJsonNode, AggregatorType extends IJsonNode> extends Aggregation<ElementType, AggregatorType> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4836890030948315219L;

	private final AggregatorType initialAggregate;

	public TransitiveAggregation(final String name, final AggregatorType initialAggregate) {
		super(name);
		this.initialAggregate = initialAggregate;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.aggregation.AggregationFunction#getFinalAggregate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode getFinalAggregate(AggregatorType aggregator, IJsonNode target) {
		return aggregator;
	}

	@SuppressWarnings("unchecked")
	@Override
	public AggregatorType initialize(AggregatorType aggregationTarget) {
		aggregationTarget = (AggregatorType) SopremoUtil.ensureType(aggregationTarget, this.initialAggregate.getClass());
		aggregationTarget.copyValueFrom(this.initialAggregate);
		return aggregationTarget;
	}
}
