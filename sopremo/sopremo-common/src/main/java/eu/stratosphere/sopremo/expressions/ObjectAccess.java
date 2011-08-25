package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.ObjectNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;

/**
 * Returns the value of an attribute of one or more Json nodes.
 * 
 * @author Arvid Heise
 */
@OptimizerHints(scope = Scope.OBJECT)
public class ObjectAccess extends EvaluationExpression implements WritableEvaluable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 357668828603766590L;

	private final String field;

	private final boolean safeDereference;

	/**
	 * Initializes FieldAccess with the given field name.
	 * 
	 * @param field
	 *        the name of the field
	 */
	public ObjectAccess(final String field) {
		this(field, false);
	}

	public ObjectAccess(String field, boolean safeDereference) {
		this.field = field;
		this.safeDereference = safeDereference;
	}

	@Override
	public EvaluationExpression asExpression() {
		return this;
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || this.getClass() != obj.getClass())
			return false;
		return this.field.equals(((ObjectAccess) obj).field);
	}

	/**
	 * If the input node is an array, the evaluation of this array performs a spread operation. In that case, the
	 * returned node is an array that contains the attribute value of each element node in the input array. In all other
	 * cases, the return value is the node associated with the field name of this FieldAccess instance or null if no
	 * such value exists.
	 */
	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		if (!node.isObject()) {
			if(node.isNull() && safeDereference) 
				return node;
			throw new EvaluationException(String.format("Cannot access field %s of non-object %s", field, node
				.getClass().getSimpleName()));
		}
		JsonNode value = node.get(this.field);
		return value == null ? NullNode.getInstance() : value;
	}

	@Override
	public int hashCode() {
		return 43 + this.field.hashCode();
	}

	@Override
	public JsonNode set(final JsonNode node, final JsonNode value, final EvaluationContext context) {
		if (!node.isObject())
			throw new EvaluationException("Cannot access field of non-object " + node.getClass().getSimpleName());
		((ObjectNode) node).put(this.field, value);
		return node;
	}

	@Override
	protected void toString(final StringBuilder builder) {
		builder.append('.').append(this.field);
	}
}