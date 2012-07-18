package eu.stratosphere.sopremo;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;

/**
 * This factory provides access to all classes located in the same package like {@link EvaluationExpression}.
 */
public class ExpressionFactory {

	/**
	 * Determines the Class-Object of the class with the given name.
	 * 
	 * @param text
	 *        the name of the class
	 * @return the class object
	 */
	public Class<?> getExpressionType(final String text) {
		try {
			return Class.forName(String.format("%s.%s", EvaluationExpression.class.getPackage().getName(), text));
		} catch (final ClassNotFoundException e) {
			throw new IllegalStateException("Unknown expression type " + text, e);
		}
	}

}
