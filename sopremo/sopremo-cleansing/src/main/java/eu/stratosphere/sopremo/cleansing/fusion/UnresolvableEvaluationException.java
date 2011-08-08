package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.EvaluationException;

public class UnresolvableEvaluationException extends EvaluationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1375112839582785405L;

	public UnresolvableEvaluationException() {
		super();
	}

	public UnresolvableEvaluationException(final String message) {
		super(message);
	}

	public UnresolvableEvaluationException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public UnresolvableEvaluationException(final Throwable cause) {
		super(cause);
	}

}
