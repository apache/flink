package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.ExpressionTag;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;

public class JsonStreamExpression extends UnevaluableExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4195183303903303669L;

	public static final ExpressionTag THIS_CONTEXT = new ExpressionTag("This");

	private final JsonStream stream;

	private final int inputIndex;

	public JsonStreamExpression(final JsonStream stream) {
		this(stream, -1);
	}

	public JsonStreamExpression(final JsonStream stream, final int inputIndex) {
		super("JsonStream placeholder");
		this.stream = stream;
		this.inputIndex = inputIndex;
	}

	/**
	 * Returns the inputIndex.
	 * 
	 * @return the inputIndex
	 */
	public int getInputIndex() {
		return this.inputIndex;
	}

	public JsonStream getStream() {
		return this.stream;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.UnevaluableExpression#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(final StringBuilder builder) {
		this.appendTags(builder);
		builder.append(this.stream.getSource().getOperator().getName()).
			append("@").append(this.stream.getSource().getIndex());
	}

	/**
	 * @param inputs
	 * @return
	 */
	public EvaluationExpression toInputSelection(final Operator<?> operator) {
		InputSelection inputSelection;
		if (this.inputIndex != -1)
			inputSelection = new InputSelection(this.inputIndex);
		else if (operator.getSource() == this.stream.getSource())
			inputSelection = new InputSelection(0);
		else {
			final int index = operator.getInputs().indexOf(this.stream.getSource());
			if (index == -1)
				return this;
			inputSelection = new InputSelection(index);
		}
		for (final ExpressionTag tag : this.getTags())
			inputSelection.addTag(tag);
		return inputSelection;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.stream.getSource().hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final JsonStreamExpression other = (JsonStreamExpression) obj;
		return this.stream.getSource().equals(other.stream.getSource());
	}

}
