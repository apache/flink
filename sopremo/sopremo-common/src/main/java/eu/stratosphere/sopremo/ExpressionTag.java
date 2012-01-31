package eu.stratosphere.sopremo;

public class ExpressionTag {
	private final String name;

	private final boolean semantic;

	public static final ExpressionTag RETAIN = new ExpressionTag("Retain", true);

	public ExpressionTag(final String name) {
		this(name, false);
	}

	public ExpressionTag(final String name, final boolean semantic) {
		this.name = name;
		this.semantic = semantic;
	}

	/**
	 * Returns the annotation.
	 * 
	 * @return the annotation
	 */
	public boolean isSemantic() {
		return this.semantic;
	}

	@Override
	public String toString() {
		return this.name;
	}
}
