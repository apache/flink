package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.JsonStream;

public class JsonStreamExpression extends ErroneousExpression {
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
}
