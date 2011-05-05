package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

public class ValueAssignment extends Mapping {
	public static final String COPY_ALL_FIELDS = "*";

	private EvaluableExpression expression;

	public ValueAssignment(String target, EvaluableExpression transformation) {
		super(target);
		this.expression = transformation;
	}

	public ValueAssignment(EvaluableExpression transformation) {
		this(NO_TARGET, transformation);
	}

	public EvaluableExpression getTransformation() {
		return this.expression;
	}

	public void setTransformation(EvaluableExpression transformation) {
		if (transformation == null)
			throw new NullPointerException("transformation must not be null");

		this.expression = transformation;
	}

	@Override
	protected void toString(StringBuilder builder) {
		if (this.getTarget() != NO_TARGET)
			builder.append(this.getTarget()).append("=");
		this.expression.toString(builder);
	}

	@Override
	public int hashCode() {
		final int prime = 61;
		int result = super.hashCode();
		result = prime * result + this.expression.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		ValueAssignment other = (ValueAssignment) obj;
		return super.equals(obj) && this.expression.equals(other.expression);
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		return this.expression.evaluate(node);
	}
}