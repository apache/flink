package eu.stratosphere.sopremo.aggregation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.PactJsonObject;

public abstract class TransitiveAggregationFunction extends AggregationFunction {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4836890030948315219L;
	private transient JsonNode aggregate, initialAggregate;

	public TransitiveAggregationFunction(String name, JsonNode initialAggregate) {
		super(name);
		this.initialAggregate = initialAggregate;
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
		new PactJsonObject(this.initialAggregate).write(oos);
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		PactJsonObject pactJsonObject = new PactJsonObject();
		pactJsonObject.read(ois);
		this.initialAggregate = pactJsonObject.getValue();
	}

	@Override
	public void initialize() {
		try {
			ByteArrayOutputStream cloneBuffer = new ByteArrayOutputStream();
			PactJsonObject cloner = new PactJsonObject(this.initialAggregate);
			cloner.write(new DataOutputStream(cloneBuffer));
			cloner.read(new DataInputStream(new ByteArrayInputStream(cloneBuffer.toByteArray())));
			this.aggregate = cloner.getValue();
		} catch (IOException e) {
			throw new IllegalStateException("Cannot clone initial value");
		}
	}

	@Override
	public void aggregate(JsonNode node, EvaluationContext context) {
		this.aggregate = this.aggregate(this.aggregate, node, context);
	}

	protected abstract JsonNode aggregate(JsonNode aggregate, JsonNode node, EvaluationContext context);

	@Override
	public JsonNode getFinalAggregate() {
		return this.aggregate;
	}
}
