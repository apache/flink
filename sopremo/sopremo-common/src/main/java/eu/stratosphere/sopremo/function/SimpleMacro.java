package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class SimpleMacro<In extends EvaluationExpression> extends MacroBase {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3337516676649699296L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public EvaluationExpression call(final EvaluationExpression[] params, final EvaluationExpression target,
			final EvaluationContext context) {
		return this.call((In) params[0], context);
	}

	public abstract EvaluationExpression call(In inputExpr, EvaluationContext context);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("Simple macro");
	}
}
