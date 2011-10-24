package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.function.MethodBase;
import eu.stratosphere.sopremo.type.JsonNode;

@OptimizerHints(scope = Scope.ANY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND)
public class MethodCall extends ContainerExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 90022725022477041L;

	private final String function;

	private final EvaluationExpression[] paramExprs;

	private final EvaluationExpression target;

	public final static EvaluationExpression NO_TARGET = new EvaluationExpression() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5225392174862839012L;

		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			return null;
		}
	};

	public MethodCall(final String function, final EvaluationExpression target, final EvaluationExpression... params) {
		this.function = function;
		this.paramExprs = params;
		this.target = target;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.function.equals(((MethodCall) obj).function)
			&& this.target.equals(((MethodCall) obj).target)
			&& Arrays.equals(this.paramExprs, ((MethodCall) obj).paramExprs);
	}

	@Override
	public int hashCode() {
		int hash = 1;		
		hash = hash * 53 + this.function.hashCode();
		hash = hash * 53 + this.target.hashCode();
		hash = hash * 53 + Arrays.hashCode(this.paramExprs);
		return hash;
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		final JsonNode target = this.target.evaluate(node, context);

		final JsonNode[] params = new JsonNode[this.paramExprs.length];
		for (int index = 0; index < params.length; index++)
			params[index] = this.paramExprs[index].evaluate(node, context);

		return context.getFunctionRegistry().evaluate(function, target, JsonUtil.asArray(params), context);
	}

	@Override
	public Iterator<EvaluationExpression> iterator() {
		return Arrays.asList(this.paramExprs).iterator();
	}

	@Override
	protected void toString(final StringBuilder builder) {
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