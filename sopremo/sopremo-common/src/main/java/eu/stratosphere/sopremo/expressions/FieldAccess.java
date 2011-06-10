package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.util.ConversionIterator;

/**
 * Returns the value of an attribute of one or more Json nodes.
 * 
 * @author Arvid Heise
 */
public class FieldAccess extends EvaluableExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 357668828603766590L;

	private final String field;

	/**
	 * Initializes FieldAccess with the given field name.
	 * 
	 * @param field
	 *        the name of the field
	 */
	public FieldAccess(String field) {
		this.field = field;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.field.equals(((FieldAccess) obj).field);
	}

	/**
	 * If the input node is an array, the evaluation of this array performs a spread operation. In that case, the
	 * returned node is an array that contains the attribute value of each element node in the input array. In all other
	 * cases, the return value is the node associated with the field name of this FieldAccess instance or null if no
	 * such value exists.
	 */
	@Override
	public JsonNode evaluate(final JsonNode node, EvaluationContext context) {
		if (node.isArray()) {
			// lazy spread
			if (node instanceof StreamArrayNode)
				return new StreamArrayNode(new ConversionIterator<JsonNode, JsonNode>(node.iterator()) {
					@Override
					protected JsonNode convert(JsonNode inputObject) {
						if (!inputObject.isObject())
							throw new EvaluationException("Cannot access field of primitive json type");
						return inputObject.get(FieldAccess.this.field);
					}
				});
			// spread
			ArrayNode arrayNode = new ArrayNode(JsonUtil.NODE_FACTORY);
			for (int index = 0, size = node.size(); index < size; index++) {
				if (!node.get(index).isObject())
					throw new EvaluationException("Cannot access field of primitive json type");
				arrayNode.add(node.get(index).get(this.field));
			}
			return arrayNode;
		} else if (!node.isObject())
			throw new EvaluationException("Cannot access field of primitive json type");
		return node.get(this.field);
	}

	@Override
	public int hashCode() {
		return 43 + this.field.hashCode();
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append('.').append(this.field);
	}
}