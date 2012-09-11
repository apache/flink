package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;

@Name(verb = "filter")
@InputCardinality(1)
public class Selection extends ElementaryOperator<Selection> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7687925343684319311L;

	private BooleanExpression condition = new UnaryExpression(new ConstantExpression(true));

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

		this.condition = (BooleanExpression) BooleanExpression.ensureBooleanExpression(condition).clone();
		this.condition.remove(new InputSelection(0));
	}

	public Selection withCondition(BooleanExpression condition) {
		this.setCondition(condition);
		return this;
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" on ").append(this.condition);
		return builder.toString();
	}


	public static class Implementation extends SopremoMap {
		private BooleanExpression condition;

		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			if (this.condition.evaluate(value, null, this.getContext()) == BooleanNode.TRUE)
				out.collect(value);
		}

	}
}
