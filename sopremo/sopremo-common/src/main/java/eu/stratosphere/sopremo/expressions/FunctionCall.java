package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.BuiltinFunctions;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtils;

public class FunctionCall extends EvaluableExpression {

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
	public JsonNode evaluate(JsonNode node) {
		// System.err.println("undefined function " + this.name);
		JsonNode[] params = new JsonNode[paramExprs.length];
		for (int index = 0; index < params.length; index++) 
			params[index] = paramExprs[index].evaluate(node);
		if (name.equals("count"))
			return BuiltinFunctions.count(JsonUtils.asArray(params));
		if (name.equals("sum"))
			return BuiltinFunctions.sum(JsonUtils.asArray(params));

		throw new EvaluationException("undefined function " + this.name);
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
		return this.name.equals(((FunctionCall) obj).name) && Arrays.equals(this.paramExprs, ((FunctionCall) obj).paramExprs);
	}

}