package eu.stratosphere.sopremo.expressions;

public abstract class SingletonExpression extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4108217673663116837L;

	private final String textualRepresentation;

	/**
	 * Initializes SingletonExpression.
	 */
	public SingletonExpression(final String textualRepresentation) {
		this.textualRepresentation = textualRepresentation;
	}

	@Override
	public boolean equals(final Object obj) {
		return obj == this;
	}

	@Override
	public int hashCode() {
		return this.getClass().hashCode();
	}

	protected abstract Object readResolve();

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(final StringBuilder builder) {
		super.toString(builder);
		builder.append(this.textualRepresentation);
	}
}
