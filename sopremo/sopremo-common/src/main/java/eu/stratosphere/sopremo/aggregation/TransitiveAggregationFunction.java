package eu.stratosphere.sopremo.aggregation;

import java.io.ByteArrayOutputStream;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public abstract class TransitiveAggregationFunction extends AggregationFunction {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4836890030948315219L;

	private transient JsonNode aggregate;

	private JsonNode initialAggregate;

	public TransitiveAggregationFunction(final String name, final JsonNode initialAggregate) {
		super(name);
		this.initialAggregate = initialAggregate;
	}

	@Override
	public void aggregate(final JsonNode node, final EvaluationContext context) {
		this.aggregate = this.aggregate(this.aggregate, node, context);
	}

	protected abstract JsonNode aggregate(JsonNode aggregate, JsonNode node, EvaluationContext context);

	@Override
	public JsonNode getFinalAggregate() {
		return this.aggregate;
	}

	@Override
	public void initialize() {
		final ByteArrayOutputStream cloneBuffer = new ByteArrayOutputStream();
		final JsonNode cloner = this.initialAggregate.clone();
		this.aggregate = cloner;
	}
}
