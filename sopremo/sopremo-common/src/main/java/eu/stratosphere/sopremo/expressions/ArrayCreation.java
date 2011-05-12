package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.Evaluable;

public class ArrayCreation extends EvaluableExpression {
	private Evaluable[] elements;

	public ArrayCreation(Evaluable... elements) {
		this.elements = elements;
	}

	public ArrayCreation(List<EvaluableExpression> elements) {
		this.elements = elements.toArray(new EvaluableExpression[elements.size()]);
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(Arrays.toString(this.elements));
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		ArrayNode arrayNode = NODE_FACTORY.arrayNode();
		for (Evaluable expression : this.elements)
			arrayNode.add(expression.evaluate(node));
		return arrayNode;
	}

	@Override
	public int hashCode() {
		return 53 + Arrays.hashCode(this.elements);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return Arrays.equals(this.elements, ((ArrayCreation) obj).elements);
	}
}