package eu.stratosphere.sopremo;

/**
 * Runtime exception that is thrown when an error during evaluation occurred.
 * 
 * @author Arvid Heise
 */
public class EvaluationException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7733621844391517408L;

	/**
	 * Initializes EvaluationException with an empty detail message and no cause.
	 */
	public EvaluationException() {
		super();
	}

	/**
	 * Initializes EvaluationException with the given detail message and no cause.
	 * 
	 * @param message
	 *        the detail message of this exception
	 */
	public EvaluationException(String message) {
		super(message);
	}

	/**
	 * Initializes EvaluationException with the given detail message and cause.
	 * 
	 * @param message
	 *        the detail message of this exception
	 * @param cause
	 *        the cause for this EvaluationException or null
	 */
	public EvaluationException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Initializes EvaluationException with an empty detail message and the given cause.
	 * 
	 * @param cause
	 *        the cause for this EvaluationException or null
	 */
	public EvaluationException(Throwable cause) {
		super(cause);
	}

}
