package eu.stratosphere.sopremo.base;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.IdentifierAccess;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class Projection extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2170992457478875950L;

	private EvaluableExpression keyTransformation, valueTransformation;

	public Projection(EvaluableExpression keyTransformation, EvaluableExpression valueTransformation, JsonStream input) {
		super(input);
		if (valueTransformation == null || keyTransformation == null)
			throw new NullPointerException();
		this.keyTransformation = keyTransformation;
		this.valueTransformation = valueTransformation;
	}

	public Projection(EvaluableExpression valueTransformation, JsonStream input) {
		this(EvaluableExpression.SAME_KEY, valueTransformation, input);
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		PactModule module = new PactModule(this.toString(), 1, 1);
		MapContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> projectionMap =
			new MapContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject>(
				ProjectionStub.class);
		module.getOutput(0).setInput(projectionMap);
		projectionMap.setInput(module.getInput(0));
		SopremoUtil.setContext(projectionMap.getStubParameters(), context);
		SopremoUtil.serialize(projectionMap.getStubParameters(), "keyTransformation", this.getKeyTransformation());
		SopremoUtil.serialize(projectionMap.getStubParameters(), "valueTransformation", this.getValueTransformation());
		return module;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.keyTransformation.hashCode();
		result = prime * result + this.valueTransformation.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Projection other = (Projection) obj;
		return this.keyTransformation.equals(other.keyTransformation)
			&& this.valueTransformation.equals(other.valueTransformation);
	}

	/**
	 * Sets the transformation of this operation that is applied to an input tuple to generate the output value.
	 * 
	 * @param valueTransformation
	 *        the new transformation of this operation
	 */
	public void setValueTransformation(EvaluableExpression valueTransformation) {
		if (valueTransformation == null)
			throw new NullPointerException("valueTransformation must not be null");

		this.valueTransformation = valueTransformation;
	}

	/**
	 * Sets the transformation of this operation that is applied to an input tuple to generate the output key.
	 * 
	 * @param keyTransformation
	 *        the new transformation of this operation
	 */
	public void setKeyTransformation(EvaluableExpression keyTransformation) {
		if (keyTransformation == null)
			throw new NullPointerException("keyTransformation must not be null");

		this.keyTransformation = keyTransformation;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" to (").append(this.getKeyTransformation()).append(", ")
				.append(this.getValueTransformation()).append(")");
		return builder.toString();
	}

	/**
	 * Returns the transformation of this operation that is applied to an input tuple to generate the output key.
	 * 
	 * @return the key transformation of this operation
	 */
	public EvaluableExpression getKeyTransformation() {
		return this.keyTransformation;
	}

	/**
	 * Returns the transformation of this operation that is applied to an input tuple to generate the output value.
	 * 
	 * @return the value transformation of this operation
	 */
	public EvaluableExpression getValueTransformation() {
		return this.valueTransformation;
	}

	public static class ProjectionStub extends
			SopremoMap<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {

		private EvaluableExpression keyTransformation, valueTransformation;

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			this.keyTransformation = SopremoUtil
				.deserialize(parameters, "keyTransformation", EvaluableExpression.class);
			this.valueTransformation = SopremoUtil.deserialize(parameters, "valueTransformation",
				EvaluableExpression.class);
		}

		@Override
		public void map(PactJsonObject.Key key, PactJsonObject value, Collector<PactJsonObject.Key, PactJsonObject> out) {
			System.out.print(key + " / " + value);
			if (this.keyTransformation != EvaluableExpression.SAME_KEY)
				key = PactJsonObject.keyOf(this.keyTransformation.evaluate(value.getValue(), this.getContext()));
			if (this.keyTransformation != EvaluableExpression.SAME_VALUE)
				value = new PactJsonObject(this.valueTransformation.evaluate(value.getValue(), this.getContext()));
			System.out.println(" -> " + key + " / " + value);
			out.collect(key, value);
		}
	}

}
