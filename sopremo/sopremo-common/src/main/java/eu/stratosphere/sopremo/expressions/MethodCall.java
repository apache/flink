package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.function.SopremoMethod;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * Calls the specified function with the provided parameters and returns the result.
 */
@OptimizerHints(scope = Scope.ANY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND)
public class MethodCall extends ContainerExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 90022725022477041L;

	private final String function;

	private List<CachingExpression<IJsonNode>> paramExprs;

	/**
	 * Initializes a MethodCall with the given function name and expressions which evaluate to the method parameters.
	 * 
	 * @param function
	 *        the name of the function that should be called
	 * @param params
	 *        expressions which evaluate to the method parameters
	 */
	public MethodCall(final String function, final EvaluationExpression... params) {
		this.function = function;
		this.paramExprs = CachingExpression.listOfAny(Arrays.asList(params));
	}

	/**
	 * Initializes a MethodCall with the given function name and expressions which evaluate to the method parameters.
	 * 
	 * @param function
	 *        the name of the function that should be called
	 * @param params
	 *        expressions which evaluate to the method parameters
	 */
	public MethodCall(final String function, final List<EvaluationExpression> params) {
		this.function = function;
		this.paramExprs = CachingExpression.listOfAny(params);
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final MethodCall other = (MethodCall) obj;
		return this.function.equals(other.function) && this.paramExprs.equals(other.paramExprs);
	}

	@Override
	public int hashCode() {
		int hash = super.hashCode();
		hash = hash * 53 + this.function.hashCode();
		hash = hash * 53 + this.paramExprs.hashCode();
		return hash;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		final IJsonNode[] params = new IJsonNode[this.paramExprs.size()];
		for (int index = 0; index < params.length; index++)
			params[index] = this.paramExprs.get(index).evaluate(node, context);

		final SopremoMethod method = context.getFunctionRegistry().get(this.function);
		if (method == null)
			throw new EvaluationException(String.format("Unknown function %s", this.function));
		return method.call(JsonUtil.asArray(params), target, context);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#getChildren()
	 */
	@Override
	public List<? extends EvaluationExpression> getChildren() {
		return this.paramExprs;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#setChildren(java.util.List)
	 */
	@Override
	public void setChildren(final List<? extends EvaluationExpression> children) {
		this.paramExprs = CachingExpression.listOfAny(children);
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.function);
		builder.append('(');
		this.appendChildExpressions(builder, this.getChildren(), ", ");
		builder.append(')');
	}

}