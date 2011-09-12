package eu.stratosphere.simple.jaql;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class ExpressionFactory {

	public Class<?> getExpressionType(String text) {
		try {
			return Class.forName(String.format("%s.%s", EvaluationExpression.class.getPackage().getName(), text));
		} catch (ClassNotFoundException e) {
			System.out.println(System.getProperty("java.class.path"));
			throw new IllegalStateException("Unknown expression type " + text, e);
		}
	}

}
