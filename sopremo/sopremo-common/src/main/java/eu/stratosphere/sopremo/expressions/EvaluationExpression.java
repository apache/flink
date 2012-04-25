package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.ExpressionTag;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.util.IdentitySet;

/**
 * Represents all evaluable expressions.
 */
public abstract class EvaluationExpression implements Iterable<EvaluationExpression>, SerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1226647739750484403L;

	protected Class<? extends JsonNode> expectedTarget;

	/**
	 * Used for secondary information during plan creation only.
	 */
	private transient Set<ExpressionTag> tags = new IdentitySet<ExpressionTag>();

	/**
	 * Represents an expression that returns the input node without any modifications. The constant is mostly used for
	 * {@link Operator}s that do not perform any transformation to the input, such as a filter operator.
	 */
	public static final EvaluationExpression VALUE = new SingletonExpression("<value>") {

		/**
		 * 
		 */
		private static final long serialVersionUID = -6430819532311429108L;

		@Override
		public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
			return node;
		}

		@Override
		public IJsonNode set(final IJsonNode node, final IJsonNode value, final EvaluationContext context) {
			return value;
		};

		@Override
		protected Object readResolve() {
			return VALUE;
		}
	};

	public static final EvaluationExpression NULL = new ConstantExpression(NullNode.getInstance()) {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2375203649638430872L;

		private Object readResolve() {
			return EvaluationExpression.NULL;
		}
	};

	public void addTag(final ExpressionTag tag) {
		this.tags.add(tag);
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final EvaluationExpression other = (EvaluationExpression) obj;

		// return this.tags.equals(other.tags);
		return this.hasAllSemanticTags(other) && other.hasAllSemanticTags(this);
	}

	protected boolean hasAllSemanticTags(final EvaluationExpression other) {
		for (final ExpressionTag tag : this.tags)
			if (tag.isSemantic() && !other.tags.contains(tag))
				return false;
		return true;
	}

	@SuppressWarnings("unchecked")
	public <T extends EvaluationExpression> T find(final Class<T> evaluableClass) {
		for (final EvaluationExpression element : this) {
			if (evaluableClass.isInstance(element))
				return (T) element;

			if (element instanceof ContainerExpression) {
				final T subSearch = ((ContainerExpression) element).find(evaluableClass);
				if (subSearch != null)
					return subSearch;
			}
		}
		return null;
	}

	/**
	 * Evaluates the given node in the provided context.<br>
	 * The given node can either be a normal {@link JsonNode} or one of the following special nodes:
	 * <ul>
	 * <li>{@link CompactArrayNode} wrapping an array of nodes if the evaluation is performed for more than one
	 * {@link JsonStream},
	 * <li>{@link StreamArrayNode} wrapping an iterator of incoming nodes which is most likely the content of a complete
	 * {@link JsonStream} that is going to be aggregated, or
	 * <li>CompactArrayNode of StreamArrayNodes when aggregating multiple JsonStreams.
	 * </ul>
	 * <br>
	 * Consequently, the result may also be of one of the previously mentioned types.<br>
	 * The ContextType provides additional information that is relevant for the evaluation, for instance all registered
	 * functions in the {@link FunctionRegistry}.
	 * 
	 * @param node
	 *        the node that should be evaluated or a special node representing containing several nodes
	 * @param target
	 *        the target that should be used
	 * @param context
	 *        the context in which the node should be evaluated
	 * @return the node resulting from the evaluation or several nodes wrapped in a special node type
	 */
	public abstract IJsonNode evaluate(IJsonNode node, IJsonNode target, EvaluationContext context);

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		for (final ExpressionTag tag : this.tags)
			if (tag.isSemantic())
				result = prime * result + tag.hashCode();
		return result;
	}

	public boolean hasTag(final ExpressionTag tag) {
		return this.tags.contains(tag);
	}

	@SuppressWarnings("unused")
	public void inferSchema(final JsonSchema requiredInput, final JsonSchema output, final EvaluationContext context) {

	}

	@Override
	public Iterator<EvaluationExpression> iterator() {
		return Arrays.asList(this).iterator();
	}

	public boolean removeTag(final ExpressionTag preserve) {
		return this.tags.remove(preserve);
	}

	/**
	 * Sets the value of the node specified by this expression using the given {@link EvaluationContext}.
	 * 
	 * @param node
	 *        the node to change
	 * @param value
	 *        the value to set
	 * @param context
	 *        the current <code>EvaluationContext</code>
	 * @return the node or a new node if the expression directly accesses the node
	 */
	@SuppressWarnings("unused")
	public IJsonNode set(final IJsonNode node, final IJsonNode value, final EvaluationContext context) {
		throw new UnsupportedOperationException(String.format(
			"Cannot change the value with expression %s of node %s to %s", this, node, value));
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		this.toString(builder);
		return builder.toString();
	}

	/**
	 * Appends a string representation of this expression to the builder. The method should return the same result as
	 * {@link #toString()} but provides a better performance when a string is composed of several child expressions.
	 * 
	 * @param builder
	 *        the builder to append to
	 */
	@Override
	public void toString(final StringBuilder builder) {
		this.appendTags(builder);
	}

	protected void appendTags(final StringBuilder builder) {
		for (final ExpressionTag tag : this.tags)
			if (tag.isSemantic())
				builder.append(tag).append(" ");
	}

	public EvaluationExpression withTag(final ExpressionTag tag) {
		this.addTag(tag);
		return this;
	}

	public Set<ExpressionTag> getTags() {
		return this.tags;
	}
}
