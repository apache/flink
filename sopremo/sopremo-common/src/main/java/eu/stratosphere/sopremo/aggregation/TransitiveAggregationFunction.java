package eu.stratosphere.sopremo.aggregation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;

public abstract class TransitiveAggregationFunction extends AggregationFunction {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4836890030948315219L;

	private transient JsonNode aggregate, initialAggregate;

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
		try {
			final ByteArrayOutputStream cloneBuffer = new ByteArrayOutputStream();
			final JsonNode cloner = this.initialAggregate;
			cloner.write(new DataOutputStream(cloneBuffer));
			cloner.read(new DataInputStream(new ByteArrayInputStream(cloneBuffer.toByteArray())));
			this.aggregate = cloner;
		} catch (final IOException e) {
			throw new IllegalStateException("Cannot clone initial value");
		}
	}

	private void readObject(final ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();		
		// TOFIX
		final JsonNode jsonObject = new ObjectNode();
		jsonObject.read(ois);
		this.initialAggregate = jsonObject;
	}

	private void writeObject(final ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		this.initialAggregate.write(oos);
	}
}
