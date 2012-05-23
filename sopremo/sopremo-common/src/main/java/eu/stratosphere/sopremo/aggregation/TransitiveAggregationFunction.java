package eu.stratosphere.sopremo.aggregation;

import eu.stratosphere.sopremo.EvaluationContext;
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

	private transient IJsonNode aggregate;

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
	 * Aggregates by passing the last result and the given {@link IJsonNode} to the aggregate implementation of the
	 * subclass
	 */
	@Override
	public void aggregate(final IJsonNode node, final EvaluationContext context) {
		this.aggregate = this.aggregate(this.aggregate, node, context);
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
