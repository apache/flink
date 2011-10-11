package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

@OptimizerHints(scope = Scope.ANY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND)
public class MethodCall extends ContainerExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 90022725022477041L;

	private final String name;

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

	public MethodCall(final String name, final EvaluationExpression target, final EvaluationExpression... params) {
		this.name = name;
		this.paramExprs = params;
		this.target = target;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.name.equals(((MethodCall) obj).name)
			&& this.target.equals(((MethodCall) obj).target)
			&& Arrays.equals(this.paramExprs, ((MethodCall) obj).paramExprs);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		final JsonNode target = this.target.evaluate(node, context);
		
		final JsonNode[] params = new JsonNode[this.paramExprs.length];
		for (int index = 0; index < params.length; index++)
			params[index] = this.paramExprs[index].evaluate(node, context);

		return context.getFunctionRegistry().evaluate(this.name, target, JsonUtil.asArray(params), context);
	}

	@Override
	public int hashCode() {
		return (53 + this.name.hashCode()) * 53 + Arrays.hashCode(this.paramExprs);
	}

	@Override
	public Iterator<EvaluationExpression> iterator() {
		return Arrays.asList(this.paramExprs).iterator();
	}

	@Override
	protected void toString(final StringBuilder builder) {
		builder.append(this.name);
		builder.append('(');
		for (int index = 0; index < this.paramExprs.length; index++) {
			builder.append(this.paramExprs[index]);
			if (index < this.paramExprs.length - 1)
				builder.append(", ");
		}
		builder.append(')');
	}

}