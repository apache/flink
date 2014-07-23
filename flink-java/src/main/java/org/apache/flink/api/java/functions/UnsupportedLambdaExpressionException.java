package org.apache.flink.api.java.functions;

/**
 * Created by kostas on 20/07/14.
 */
public class UnsupportedLambdaExpressionException extends RuntimeException {

	private static final long serialVersionUID = -1721898801986321010L;

	public UnsupportedLambdaExpressionException(String message) {
		super(message);
	}
}
