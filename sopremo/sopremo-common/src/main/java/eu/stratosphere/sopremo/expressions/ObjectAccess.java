package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * Returns the value of an attribute of one or more Json nodes.
 * 
 * @author Arvid Heise
 */
@OptimizerHints(scope = Scope.OBJECT)
public class ObjectAccess extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = 357668828603766590L;

	private final String field;

	private final boolean safeDereference;

	/**
	 * Initializes ObjectAccess with the given field name.
	 * 
	 * @param field
	 *        the name of the field
	 */
	public ObjectAccess(final String field) {
		this(field, false);
	}

	/**
	 * Initializes ObjectAccess with the given field name and the given safeDereference flag.
	 * 
	 * @param field
	 *        the name of the field
	 * @param safeDereference
	 *        determines either 'null' representing nodes should be evaluated or not
	 */
	public ObjectAccess(final String field, final boolean safeDereference) {
		this.field = field;
		this.safeDereference = safeDereference;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ObjectAccess other = (ObjectAccess) obj;
		return this.field.equals(other.field);
	}

	/**
	 * Returns the field.
	 * 
	 * @return the field
	 */
	public String getField() {
		return this.field;
	}

	/**
	 * If the input node is an array, the evaluation of this array performs a spread operation. In that case, the
	 * returned node is an array that contains the attribute value of each element node in the input array. In all other
	 * cases, the return value is the node associated with the field name of this FieldAccess instance or
	 * {@link NullNode} if no such value exists.
	 */
	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		if (!node.isObject()) {
			if (node.isNull() && this.safeDereference)
				return node;

			return MissingNode.getInstance();
			// throw new EvaluationException(String.format("Cannot access field %s of non-object %s", this.field, node
			// .getClass().getSimpleName()));
		}
		final IJsonNode value = ((IObjectNode) node).get(this.field);
		return value == null ? NullNode.getInstance() : value;
	}

	@Override
	public int hashCode() {
		return 43 * super.hashCode() + this.field.hashCode();
	}

	@Override
	public IJsonNode set(final IJsonNode node, final IJsonNode value, final EvaluationContext context) {
		if (!node.isObject())
			throw new EvaluationException("Cannot access field of non-object " + node.getClass().getSimpleName());
		((IObjectNode) node).put(this.field, value);
		return node;
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append('.').append(this.field);
	}
}