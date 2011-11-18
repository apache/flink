package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NumericNode;

@OptimizerHints(scope = Scope.ANY)
public class ConstantExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4270374147359826240L;

	// TODO: adjust to json model
	private JsonNode constant;

	public ConstantExpression(final JsonNode constant) {
		this.constant = constant;
	}

	public ConstantExpression(final Object constant) {
		this.constant = JsonUtil.OBJECT_MAPPER.valueToTree(constant);
	}

	public int asInt() {
		if (this.constant instanceof NumericNode)
			return ((NumericNode) this.constant).getIntValue();
		return Integer.parseInt(this.constant.toString());
	}

	public String asString() {
		return this.constant.toString();
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ConstantExpression other = (ConstantExpression) obj;
		return this.constant.equals(other.constant);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		return this.constant;
	}

	@Override
	public int hashCode() {
		return 41 + this.constant.hashCode();
	}

	@Override
	public void toString(final StringBuilder builder) {
		if (this.constant instanceof CharSequence)
			builder.append("\'").append(this.constant).append("\'");
		else
			builder.append(this.constant);
	}

}