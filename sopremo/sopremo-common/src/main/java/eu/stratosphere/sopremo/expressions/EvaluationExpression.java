package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.ExpressionTag;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.util.IdentitySet;

public abstract class EvaluationExpression implements Iterable<EvaluationExpression>, SerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1226647739750484403L;

	/**
	 * Used for secondary information during plan creation only.
	 */
	private transient Set<ExpressionTag> tags = new IdentitySet<ExpressionTag>();

	public final static EvaluationExpression KEY = new SingletonExpression("<key>") {
		/**
		 * 
		 */
		private static final long serialVersionUID = 9192628786637605317L;

		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			throw new EvaluationException();
		}

		protected Object readResolve() {
			return EvaluationExpression.KEY;
		}
	};

	public final static EvaluationExpression AS_KEY = new SingletonExpression("-><key>") {
		/**
		 * 
		 */
		private static final long serialVersionUID = 9192628786637605317L;

		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			throw new EvaluationException();
		}

		protected Object readResolve() {
			return EvaluationExpression.AS_KEY;
		}
	};

	/**
	 * Represents an expression that returns the input node without any modifications. The constant is mostly used for
	 * {@link Operator}s that do not perform any transformation to the input, such as a filter operator.
	 */
	public static final SameValueExpression VALUE = new SameValueExpression();

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
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		EvaluationExpression other = (EvaluationExpression) obj;

		// return this.tags.equals(other.tags);
		return this.hasAllSemanticTags(other) && other.hasAllSemanticTags(this);
	}

	protected boolean hasAllSemanticTags(EvaluationExpression other) {
		for (ExpressionTag tag : tags)
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
	 * @param context
	 *        the context in which the node should be evaluated
	 * @return the node resulting from the evaluation or several nodes wrapped in a special node type
	 */
	public abstract JsonNode evaluate(JsonNode node, EvaluationContext context);

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		for (ExpressionTag tag : tags)
			if (tag.isSemantic())
				result = prime * result + tag.hashCode();
		return result;
	}

	public boolean hasTag(final ExpressionTag tag) {
		return this.tags.contains(tag);
	}

	public void inferSchema(JsonSchema requiredInput, JsonSchema output, EvaluationContext context) {

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
	public JsonNode set(JsonNode node, JsonNode value, EvaluationContext context) {
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
	public void toString(StringBuilder builder) {
		appendTags(builder);
	}

	protected void appendTags(StringBuilder builder) {
		for (ExpressionTag tag : this.tags)
			if (tag.isSemantic())
				builder.append(tag).append(" ");
	}

	public EvaluationExpression withTag(final ExpressionTag tag) {
		this.addTag(tag);
		return this;
	}

	public Set<ExpressionTag> getTags() {
		return tags;
	}

	private static final class SameValueExpression extends EvaluationExpression {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6957283445639387461L;

		@Override
		public boolean equals(final Object obj) {
			return this == obj;
		}

		/**
		 * Returns the node without modifications.
		 */
		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			return node;
		}

		@Override
		public int hashCode() {
			return System.identityHashCode(this);
		}

		private Object readResolve() {
			return EvaluationExpression.VALUE;
		}

		@Override
		public JsonNode set(final JsonNode node, final JsonNode value, final EvaluationContext context) {
			return value;
		}

		@Override
		public void toString(final StringBuilder builder) {
			builder.append("<value>");
		}
	}
}
