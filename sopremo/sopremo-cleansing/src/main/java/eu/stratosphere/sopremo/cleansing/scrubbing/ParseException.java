package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.EvaluationException;

public class ParseException extends EvaluationException {

	public ParseException() {
		super();
	}

	public ParseException(String message, Throwable cause) {
		super(message, cause);
	}

	public ParseException(String message) {
		super(message);
	}

	public ParseException(Throwable cause) {
		super(cause);
	}

}
