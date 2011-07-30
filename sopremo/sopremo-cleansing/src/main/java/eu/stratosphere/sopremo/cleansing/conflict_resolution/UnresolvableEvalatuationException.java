package eu.stratosphere.sopremo.cleansing.conflict_resolution;

import eu.stratosphere.sopremo.EvaluationException;

public class UnresolvableEvalatuationException extends EvaluationException {

	public UnresolvableEvalatuationException() {
		super();
	}

	public UnresolvableEvalatuationException(String message, Throwable cause) {
		super(message, cause);
	}

	public UnresolvableEvalatuationException(String message) {
		super(message);
	}

	public UnresolvableEvalatuationException(Throwable cause) {
		super(cause);
	}

}
