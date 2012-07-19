package eu.stratosphere.sopremo.type;

import eu.stratosphere.sopremo.EvaluationException;

public class CoercionException extends EvaluationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1067147890265712537L;

	public CoercionException() {
		super();
	}

	public CoercionException(final String message) {
		super(message);
	}

	public CoercionException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public CoercionException(final Throwable cause) {
		super(cause);
	}

}
