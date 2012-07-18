package eu.stratosphere.sopremo.aggregation;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Extend this AggregationFunction to implement functions where the result of an aggregation step serves as the input of
 * the next one
 */
public abstract class TransitiveAggregationFunction extends AggregationFunction {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4836890030948315219L;

	private final IJsonNode initialAggregate;

	/**
	 * Initializes a new TransitiveAggregationFunction with the given name and an initial aggregate.
	 * 
	 * @param name
	 *        the name that should be used
	 * @param initialAggregate
	 *        the initial value of the aggregate
	 */
	public TransitiveAggregationFunction(final String name, final IJsonNode initialAggregate) {
		super(name);
		this.initialAggregate = initialAggregate;
	}

	/**
	 * This method must be implemented to specify how this function aggregates the nodes
	 * 
	 * @param aggregate
	 *        the aggregate that should be used
	 * @param node
	 *        the node that should be added to the aggregate
	 * @param context
	 *        additional context informations
	 * @return the aggregate after processing
	 */
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
	public IJsonNode getFinalAggregate(final IJsonNode aggregator, final IJsonNode target) {
		return aggregator;
	}

	@Override
	public IJsonNode initialize(IJsonNode aggregationTarget) {
		aggregationTarget = SopremoUtil.ensureType(aggregationTarget, this.initialAggregate.getClass());
		aggregationTarget.copyValueFrom(this.initialAggregate);
		return aggregationTarget;
	}
}
