package eu.stratosphere.sopremo.expressions;

import java.util.List;

import eu.stratosphere.sopremo.JsonStream;

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
	public Object toInputSelection(List<JsonStream> inputs) {
		return null;
	}
}
