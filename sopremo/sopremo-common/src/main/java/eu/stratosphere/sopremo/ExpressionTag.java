package eu.stratosphere.sopremo;

public class ExpressionTag {
	private final String name;

	public static final ExpressionTag RETAIN = new ExpressionTag("Retain");

	protected ExpressionTag(final String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return this.name;
	}
}
