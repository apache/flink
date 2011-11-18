package eu.stratosphere.sopremo.expressions;

import java.util.List;

import eu.stratosphere.sopremo.ExpressionTag;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;

public class JsonStreamExpression extends UnevaluableExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4195183303903303669L;

	private JsonStream stream;
	
	public JsonStreamExpression(JsonStream stream) {
		super("JsonStream placeholder");
		this.stream = stream;
	}

	public JsonStream getStream() {
		return this.stream;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.UnevaluableExpression#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append(this.stream.getSource().getOperator().getName()).
			append("@").append(this.stream.getSource().getIndex());
	}

	/**
	 * @param inputs
	 * @return
	 */
	public EvaluationExpression toInputSelection(Operator<?> operator) {
		if(operator == stream)
			return new InputSelection(0);
		int index = operator.getInputs().indexOf(stream.getSource());
		if(index == -1)
			return this;
		InputSelection inputSelection = new InputSelection(index);
		for(ExpressionTag tag : this.getTags())
			inputSelection.addTag(tag);
		return inputSelection;
	}
}
