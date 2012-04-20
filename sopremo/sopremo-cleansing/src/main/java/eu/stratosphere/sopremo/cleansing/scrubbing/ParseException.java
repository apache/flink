package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.EvaluationException;

public class ParseException extends EvaluationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4050831593250170454L;

	public ParseException() {
		super();
	}

	public ParseException(final String message) {
		super(message);
	}

	public ParseException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public ParseException(final Throwable cause) {
		super(cause);
	}

}
