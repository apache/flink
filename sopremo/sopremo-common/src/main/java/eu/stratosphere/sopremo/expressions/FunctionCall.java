package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.pact.SopremoUtil;

@OptimizerHints(scope = Scope.ANY, minNodes = 0, maxNodes = OptimizerHints.UNBOUND)
public class FunctionCall extends ContainerExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 90022725022477041L;

	private final String name;

	private final SopremoExpression<EvaluationContext>[] paramExprs;

	public FunctionCall(final String name, final EvaluationExpression... params) {
		this.name = name;
		this.paramExprs = params;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.name.equals(((FunctionCall) obj).name)
			&& Arrays.equals(this.paramExprs, ((FunctionCall) obj).paramExprs);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		// System.err.println("undefined function " + this.name);
		final JsonNode[] params = new JsonNode[this.paramExprs.length];	
		for (int index = 0; index < params.length; index++)
			params[index] = this.paramExprs[index].evaluate(node, context);
//		SopremoUtil.LOG.warn(name + " " + Arrays.asList(params) + " " +Arrays.asList(paramExprs) );	
		// if (name.equals("count"))
		// return BuiltinFunctions.count(JsonUtils.asArray(params));
		// if (name.equals("sum"))
		// return BuiltinFunctions.sum(JsonUtils.asArray(params));
		//
		// throw new EvaluationException("undefined function " + this.name);

		return context.getFunctionRegistry().evaluate(this.name, JsonUtil.asArray(params), context);
	}

	@Override
	public int hashCode() {
		return (53 + this.name.hashCode()) * 53 + Arrays.hashCode(this.paramExprs);
	}

	@Override
	public Iterator<SopremoExpression<EvaluationContext>> iterator() {
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