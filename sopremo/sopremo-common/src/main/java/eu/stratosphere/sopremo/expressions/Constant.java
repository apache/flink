package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;

public class Constant extends EvaluableExpression {
	// TODO: adjust to json model
	private Object constant;

	public Constant(Object constant) {
		this.constant = constant;
	}

	public String asString() {
		return this.constant.toString();
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return JsonUtil.OBJECT_MAPPER.valueToTree(this.constant);
	}

	public int asInt() {
		if (this.constant instanceof Number)
			return ((Number) this.constant).intValue();
		return Integer.parseInt(this.constant.toString());
	}

	@Override
	protected void toString(StringBuilder builder) {
		if (this.constant instanceof CharSequence)
			builder.append("\'").append(this.constant).append("\'");
		else
			builder.append(this.constant);
	}

	@Override
	public int hashCode() {
		return 41 + this.constant.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.constant.equals(((Constant) obj).constant);
	}
}