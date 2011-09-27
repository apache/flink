package eu.stratosphere.sopremo.base;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class Projection extends ElementaryOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2170992457478875950L;

	private final EvaluationExpression keyTransformation, valueTransformation;

	public Projection(final EvaluationExpression keyTransformation, final EvaluationExpression valueTransformation,
			final JsonStream input) {
		super(input);
		if (valueTransformation == null || keyTransformation == null)
			throw new NullPointerException();
		this.keyTransformation = keyTransformation;
		this.valueTransformation = valueTransformation;
	}

	public Projection(final EvaluationExpression valueTransformation, final JsonStream input) {
		this(EvaluationExpression.KEY, valueTransformation, input);
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		if (this.keyTransformation == EvaluationExpression.KEY
			&& this.valueTransformation == EvaluationExpression.VALUE)
			return createShortCircuitModule();
		return super.asPactModule(context);
	}

	//
	// @Override
	// public PactModule asPactModule(EvaluationContext context) {
	// PactModule module = new PactModule(this.toString(), 1, 1);
	// MapContract<JsonNode, JsonNode, JsonNode, JsonNode> projectionMap =
	// new MapContract<JsonNode, JsonNode, JsonNode, JsonNode>(
	// ProjectionStub.class);
	// module.getOutput(0).setInput(projectionMap);
	// projectionMap.setInput(module.getInput(0));
	// SopremoUtil.setContext(projectionMap.getStubParameters(), context);
	// SopremoUtil.serialize(projectionMap.getStubParameters(), "keyTransformation", this.getKeyTransformation());
	// SopremoUtil.serialize(projectionMap.getStubParameters(), "valueTransformation", this.getValueTransformation());
	// return module;
	// }

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final Projection other = (Projection) obj;
		return this.keyTransformation.equals(other.keyTransformation)
			&& this.valueTransformation.equals(other.valueTransformation);
	}

	/**
	 * Returns the transformation of this operation that is applied to an input tuple to generate the output key.
	 * 
	 * @return the key transformation of this operation
	 */
	public EvaluationExpression getKeyTransformation() {
		return this.keyTransformation;
	}

	/**
	 * Returns the transformation of this operation that is applied to an input tuple to generate the output value.
	 * 
	 * @return the value transformation of this operation
	 */
	public EvaluationExpression getValueTransformation() {
		return this.valueTransformation;
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
	public String toString() {
		final StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" to (").append(this.getKeyTransformation()).append(", ")
			.append(this.getValueTransformation()).append(")");
		return builder.toString();
	}

	public static class ProjectionStub extends
			SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {

		private EvaluationExpression keyTransformation, valueTransformation;

		@Override
		protected void map(JsonNode key, JsonNode value, final JsonCollector out) {
			JsonNode outKey = key, outValue = value;
			if (this.keyTransformation != EvaluationExpression.KEY)
				outKey = this.keyTransformation.evaluate(value, this.getContext());
			if (this.valueTransformation == EvaluationExpression.KEY)
				outValue = key;
			else if (this.valueTransformation != EvaluationExpression.VALUE)
				outValue = this.valueTransformation.evaluate(value, this.getContext());
			out.collect(outKey, outValue);
		}
	}

}
