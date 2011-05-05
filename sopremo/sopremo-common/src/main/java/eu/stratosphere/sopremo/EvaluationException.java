package eu.stratosphere.sopremo;

public class EvaluationException extends RuntimeException {

	public EvaluationException() {
		super();
	}

	public EvaluationException(String message, Throwable cause) {
		super(message, cause);
	}

	public EvaluationException(String message) {
		super(message);
	}

	public EvaluationException(String message, Object... params) {
		super(String.format(message, params));
	}

	public EvaluationException(Throwable cause) {
		super(cause);
	}

}
