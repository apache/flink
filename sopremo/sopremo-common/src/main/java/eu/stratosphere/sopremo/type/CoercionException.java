package eu.stratosphere.sopremo.type;

import eu.stratosphere.sopremo.EvaluationException;

/**
 * If an exception occurs during the process of a coercion this exception should be thrown
 */
public class CoercionException extends EvaluationException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1067147890265712537L;

	/**
	 * Initializes a CoercionException.
	 */
	public CoercionException() {
		super();
	}

	/**
	 * Initializes a CoercionException with the given message.
	 * 
	 * @param message
	 *        the message that should be used
	 */
	public CoercionException(final String message) {
		super(message);
	}

	/**
	 * Initializes a CoercionException with the given message and the given {@link Throwable}.
	 * 
	 * @param message
	 *        the message that should be used
	 * @param cause
	 *        the Throwable that has caused this exception
	 */
	public CoercionException(final String message, final Throwable cause) {
		super(message, cause);
	}

	/**
	 * Initializes a CoercionException with the given {@link Throwable}.
	 * 
	 * @param cause
	 *        the Throwable that has caused this exception
	 */
	public CoercionException(final Throwable cause) {
		super(cause);
	}

}
