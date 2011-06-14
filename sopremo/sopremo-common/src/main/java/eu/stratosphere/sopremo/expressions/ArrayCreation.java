package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;

/**
 * Creates an array of the given expressions.
 * 
 * @author Arvid Heise
 */
public class ArrayCreation extends ContainerExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1681947333740209285L;

	private EvaluableExpression[] elements;

	/**
	 * Initializes ArrayCreation to create an array of the given expressions.
	 * 
	 * @param elements
	 *        the expressions that evaluate to the elements in the array
	 */
	public ArrayCreation(EvaluableExpression... elements) {
		this.elements = elements;
	}

	/**
	 * Initializes ArrayCreation to create an array of the given expressions.
	 * 
	 * @param elements
	 *        the expressions that evaluate to the elements in the array
	 */
	public ArrayCreation(List<EvaluableExpression> elements) {
		this.elements = elements.toArray(new EvaluableExpression[elements.size()]);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return Arrays.equals(this.elements, ((ArrayCreation) obj).elements);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		ArrayNode arrayNode = JsonUtil.NODE_FACTORY.arrayNode();
		for (Evaluable expression : this.elements)
			arrayNode.add(expression.evaluate(node, context));
		return arrayNode;
	}

	@Override
	public int hashCode() {
		return 53 + Arrays.hashCode(this.elements);
	}

	@Override
	public Iterator<EvaluableExpression> iterator() {
		return Arrays.asList(this.elements).iterator();
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(Arrays.toString(this.elements));
	}
}