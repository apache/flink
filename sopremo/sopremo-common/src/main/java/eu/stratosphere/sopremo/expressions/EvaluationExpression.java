package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.ExpressionTag;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.util.IdentitySet;

public abstract class EvaluationExpression implements Iterable<EvaluationExpression>, SerializableSopremoType {
	private static final class SameValueExpression extends EvaluationExpression {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6957283445639387461L;

		/**
		 * Returns the node without modifications.
		 */
		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			return node;
		}

		private Object readResolve() {
			return EvaluationExpression.VALUE;
		}

		@Override
		public boolean equals(final Object obj) {
			return this == obj;
		}

		@Override
		public JsonNode set(final JsonNode node, final JsonNode value, final EvaluationContext context) {
			return value;
		}

		@Override
		public void toString(final StringBuilder builder) {
			builder.append("<value>");
		}

		@Override
		public int hashCode() {
			return System.identityHashCode(this);
		}
	}

	@Override
	public Iterator<EvaluationExpression> iterator() {
		return Arrays.asList(this).iterator();
	}

	public void inferSchema(JsonSchema requiredInput, JsonSchema output, EvaluationContext context) {

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

	/**
	 * 
	 */
	private static final long serialVersionUID = 1226647739750484403L;

	/**
	 * Used for secondary information during plan creation only.
	 */
	private transient Set<ExpressionTag> tags = new IdentitySet<ExpressionTag>();

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

	public final static EvaluationExpression KEY = new EvaluationExpression() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 9192628786637605317L;

		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			throw new EvaluationException();
		}

		private Object readResolve() {
			return EvaluationExpression.KEY;
		}

		@Override
		public void toString(final StringBuilder builder) {
			builder.append("<key>");
		};
	};

	public final static EvaluationExpression AS_KEY = new EvaluationExpression() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 9192628786637605317L;

		@Override
		public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
			throw new EvaluationException();
		}

		private Object readResolve() {
			return EvaluationExpression.AS_KEY;
		}

		@Override
		public void toString(final StringBuilder builder) {
			builder.append("-><key>");
		};
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

	public boolean hasTag(final ExpressionTag tag) {
		return this.tags.contains(tag);
	}

	public boolean removeTag(final ExpressionTag preserve) {
		return this.tags.remove(preserve);
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
	public void toString(StringBuilder builder) {
		if (!this.tags.isEmpty())
			builder.append(this.tags).append(" ");
	}

	public EvaluationExpression withTag(final ExpressionTag tag) {
		this.addTag(tag);
		return this;
	}
}
