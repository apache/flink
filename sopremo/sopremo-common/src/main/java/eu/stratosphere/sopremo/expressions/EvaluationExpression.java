package eu.stratosphere.sopremo.expressions;

import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.util.IdentitySet;

public abstract class EvaluationExpression implements SerializableSopremoType, Evaluable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1226647739750484403L;

	/**
	 * Used for secondary information during plan creation only.
	 */
	private transient Set<ExpressionTag> tags = new IdentitySet<ExpressionTag>();

	public void addTag(ExpressionTag tag) {
		this.tags.add(tag);
	}

	public EvaluationExpression withTag(ExpressionTag tag) {
		this.addTag(tag);
		return this;
	}

	public boolean hasTag(ExpressionTag tag) {
		return this.tags.contains(tag);
	}

	public boolean removeTag(ExpressionTag preserve) {
		return this.tags.remove(preserve);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
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
	protected void toString(StringBuilder builder) {
	}

	/**
	 * Represents an unresolvable expression. The value most likely indicates a programming error and an evaluation
	 * attempt causes an exceptions.
	 */
	public static final EvaluationExpression UNKNOWN = new IdentifierAccess("?");

	public final static EvaluationExpression SAME_KEY = new EvaluationExpression() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 9192628786637605317L;

		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			throw new EvaluationException();
		}

		private Object readResolve() {
			return EvaluationExpression.SAME_KEY;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append("<key>");
		};
	};

	/**
	 * Represents an expression that returns the input node without any modifications. The constant is mostly used for
	 * {@link Operator}s that do not perform any transformation to the input, such as a filter operator.
	 */
	public static final EvaluationExpression SAME_VALUE = new EvaluationExpression() {

		/**
		 * 
		 */
		private static final long serialVersionUID = -6957283445639387461L;

		/**
		 * Returns the node without modifications.
		 */
		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			return node;
		};

		private Object readResolve() {
			return EvaluationExpression.SAME_VALUE;
		}

		@Override
		protected void toString(StringBuilder builder) {
			builder.append("<value>");
		};
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
}
