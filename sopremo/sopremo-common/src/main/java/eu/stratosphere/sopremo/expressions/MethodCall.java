package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.type.IJsonNode;

@OptimizerHints(scope = Scope.ANY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND)
public class MethodCall extends ContainerExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 90022725022477041L;

	private final String function;

	private EvaluationExpression[] paramExprs;

	public MethodCall(final String function, final EvaluationExpression... params) {
		this.function = function;
		this.paramExprs = params;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final MethodCall other = (MethodCall) obj;
		return this.function.equals(other.function)
			&& Arrays.equals(this.paramExprs, other.paramExprs);
	}

	@Override
	public int hashCode() {
		int hash = super.hashCode();
		hash = hash * 53 + this.function.hashCode();
		hash = hash * 53 + Arrays.hashCode(this.paramExprs);
		return hash;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, final EvaluationContext context) {
		final IJsonNode[] params = new IJsonNode[this.paramExprs.length];
		for (int index = 0; index < params.length; index++)
			params[index] = this.paramExprs[index].evaluate(node, context);

		return context.getFunctionRegistry().evaluate(this.function, JsonUtil.asArray(params), context);
	}

	@Override
	public Iterator<EvaluationExpression> iterator() {
		return Arrays.asList(this.paramExprs).iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#getChildren()
	 */
	@Override
	public List<? extends EvaluationExpression> getChildren() {
		return Arrays.asList(this.paramExprs);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#setChildren(java.util.List)
	 */
	@Override
	public void setChildren(final List<? extends EvaluationExpression> children) {
		this.paramExprs = children.toArray(this.paramExprs);
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.function);
		builder.append('(');
		for (int index = 0; index < this.paramExprs.length; index++) {
			builder.append(this.paramExprs[index]);
			if (index < this.paramExprs.length - 1)
				builder.append(", ");
		}
		builder.append(')');
	}

}