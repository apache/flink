package eu.stratosphere.sopremo.expressions;

public class ExpressionTag {
	private final String name;

	public static final ExpressionTag PRESERVE = new ExpressionTag("Preserve");

	protected ExpressionTag(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return this.name;
	}
}
