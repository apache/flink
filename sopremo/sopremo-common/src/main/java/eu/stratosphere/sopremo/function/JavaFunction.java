package eu.stratosphere.sopremo.function;

import java.lang.reflect.Method;
import java.util.Collection;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.util.reflect.DynamicMethod;
import eu.stratosphere.util.reflect.Signature;

public class JavaFunction extends Function {
	/**
	 * 
	 */
	private static final long serialVersionUID = -789826280721581321L;

	private DynamicMethod<JsonNode> method;

	public JavaFunction(final String name) {
		super(name);

		this.method = new DynamicMethod<JsonNode>(name);
	}

	public void addSignature(final Method method) {
		this.method.addSignature(method);
	}

	public Collection<Signature> getSignatures() {
		return this.method.getSignatures();
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		return this.method.invoke(null, this.getParams(node));
	}

	private Object[] getParams(final JsonNode node) {
		JsonNode[] params;
		if (node instanceof CompactArrayNode) {
			params = new JsonNode[node.size()];

			for (int index = 0; index < params.length; index++)
				params[index] = node.get(index);
		} else
			params = new JsonNode[] { node };
		return params;
	}

}
