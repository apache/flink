package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;

public class FunctionCall extends ContainerExpression<Evaluable> {

	private String name;

	private Evaluable[] paramExprs;

	public FunctionCall(String name, Evaluable... params) {
		this.name = name;
		this.paramExprs = params;
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(this.name);
		builder.append('(');
		for (int index = 0; index < this.paramExprs.length; index++) {
			builder.append(this.paramExprs[index]);
			if (index < this.paramExprs.length - 1)
				builder.append(", ");
		}
		builder.append(')');
	}

	@Override
	public Iterator<Evaluable> iterator() {
		return Arrays.asList(paramExprs).iterator();
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		// System.err.println("undefined function " + this.name);
		JsonNode[] params = new JsonNode[this.paramExprs.length];
		for (int index = 0; index < params.length; index++)
			params[index] = this.paramExprs[index].evaluate(node, context);
		// if (name.equals("count"))
		// return BuiltinFunctions.count(JsonUtils.asArray(params));
		// if (name.equals("sum"))
		// return BuiltinFunctions.sum(JsonUtils.asArray(params));
		//
		// throw new EvaluationException("undefined function " + this.name);

		return context.getFunctionRegistry().evaluate(this.name, JsonUtil.asArray(params), context);
	}

	//
	// @Override
	// protected JsonNode aggregate(Iterator<JsonNode> input) {

	// }

	@Override
	public int hashCode() {
		return (53 + this.name.hashCode()) * 53 + Arrays.hashCode(this.paramExprs);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.name.equals(((FunctionCall) obj).name)
			&& Arrays.equals(this.paramExprs, ((FunctionCall) obj).paramExprs);
	}

}