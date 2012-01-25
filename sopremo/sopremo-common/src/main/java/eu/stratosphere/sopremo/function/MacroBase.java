package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class MacroBase extends Callable<EvaluationExpression, EvaluationExpression[]> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 93515191173062050L;

	/**
	 * Initializes Macro.
	 * 
	 * @param name
	 * @param definition
	 */
	public MacroBase(String name) {
		super(name);
	}
	
	/**
	 * Initializes MacroBase.
	 *
	 */
	public MacroBase() {
		this("");
	}
}
