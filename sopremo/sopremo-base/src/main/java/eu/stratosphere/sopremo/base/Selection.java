package eu.stratosphere.sopremo.base;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoMap;

@Name(verb = "select")
public class Selection extends ElementaryOperator<Selection> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7687925343684319311L;

	private EvaluationExpression condition = new ConstantExpression(true);

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return super.equals(obj) && this.condition.equals(((Selection) obj).condition);
	}

	public EvaluationExpression getCondition() {
		return this.condition;
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result + this.condition.hashCode();
		return result;
	}

	@Property(preferred = true)
	@Name(preposition = "where")
	public void setCondition(EvaluationExpression condition) {
		if (condition == null)
			throw new NullPointerException("condition must not be null");

		this.condition = condition;
	}

	public Selection withCondition(EvaluationExpression condition) {
		this.setCondition(condition);
		return this;
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" on ").append(this.condition);
		return builder.toString();
	}

	public static class Implementation extends
			SopremoMap<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
		private BooleanExpression condition;

		@Override
		protected void map(final JsonNode key, final JsonNode value, final JsonCollector out) {
			if (this.condition.evaluate(value, this.getContext()) == BooleanNode.TRUE)
				out.collect(key, value);
		}

	}
}
