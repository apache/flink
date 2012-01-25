package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class SimpleMacro<In extends EvaluationExpression> extends MacroBase {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3337516676649699296L;

	/**
	 * Initializes Macro.
	 * 
	 * @param name
	 * @param definition
	 */
	public SimpleMacro(final String name) {
		super(name);
	}

	/**
	 * Initializes MacroBase.
	 */
	public SimpleMacro() {
		this("");
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public EvaluationExpression call(final EvaluationExpression[] params, final EvaluationContext context) {
		return this.call((In) params[0], context);
	}

	/**
	 * @param evaluationExpression
	 * @param context
	 * @return
	 */
	public abstract EvaluationExpression call(In inputExpr, EvaluationContext context);
}
