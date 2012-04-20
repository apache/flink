package eu.stratosphere.sopremo.expressions;

public class MethodPointerExpression extends UnevaluableExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3894350171936013804L;

	private final String functionName;

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
