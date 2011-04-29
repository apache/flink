package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;


public class Function extends EvaluableExpression {

	private String name;

	private EvaluableExpression[] params;

	public Function(String name, EvaluableExpression... params) {
		this.name = name;
		this.params = params;
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(this.name);
		builder.append('(');
		for (int index = 0; index < this.params.length; index++) {
			builder.append(this.params[index]);
			if (index < this.params.length - 1)
				builder.append(", ");
		}
		builder.append(')');
	}

	@Override
	public int hashCode() {
		return (53 + this.name.hashCode()) * 53 + Arrays.hashCode(this.params);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.name.equals(((Function) obj).name) && Arrays.equals(this.params, ((Function) obj).params);
	}
}