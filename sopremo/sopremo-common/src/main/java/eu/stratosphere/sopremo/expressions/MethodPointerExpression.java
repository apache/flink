package eu.stratosphere.sopremo.expressions;

/**
 * This expression represents a pointer to a method.
 */
public class MethodPointerExpression extends UnevaluableExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3894350171936013804L;

	private final String functionName;

	/**
	 * Initializes a MethodPointerExpression with the given function name.
	 * 
	 * @param functionName
	 *        a name of function
	 */
	public MethodPointerExpression(final String functionName) {
		super("&" + functionName);
		this.functionName = functionName;
	}

	/**
	 * Returns the functionName.
	 * 
	 * @return the functionName
	 */
	public String getFunctionName() {
		return this.functionName;
	}
}
