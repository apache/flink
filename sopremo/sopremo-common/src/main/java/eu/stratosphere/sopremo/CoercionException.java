package eu.stratosphere.sopremo;

public class CoercionException extends EvaluationException {

	public CoercionException() {
		super();
	}

	public CoercionException(String message, Throwable cause) {
		super(message, cause);
	}

	public CoercionException(String message) {
		super(message);
	}

	public CoercionException(Throwable cause) {
		super(cause);
	}

}
