package eu.stratosphere.sopremo.base;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IJsonNode;

@Name(verb = "transform")
public class Projection extends ElementaryOperator<Projection> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2170992457478875950L;

	private EvaluationExpression transformation = EvaluationExpression.VALUE;

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		if (this.transformation == EvaluationExpression.VALUE)
			return this.createShortCircuitModule();
		return super.asPactModule(context);
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final Projection other = (Projection) obj;
		return this.transformation.equals(other.transformation);
	}

	/**
	 * Returns the transformation of this operation that is applied to an input tuple to generate the output value.
	 * 
	 * @return the value transformation of this operation
	 */
	public EvaluationExpression getTransformation() {
		return this.transformation;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.transformation.hashCode();
		return result;
	}

	@Property(preferred = true)
	@Name(preposition = "into")
	public void setTransformation(EvaluationExpression transformation) {
		if (transformation == null)
			throw new NullPointerException("transformation must not be null");

		this.transformation = transformation;
	}

	public Projection withTransformation(EvaluationExpression transformation) {
		this.setTransformation(transformation);
		return this;
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" to ").append(this.getTransformation());
		return builder.toString();
	}

	public static class ProjectionStub extends SopremoMap {
		private CachingExpression<IJsonNode> transformation;

		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			out.collect(this.transformation.evaluate(value, this.getContext()));
		}
	}

}
