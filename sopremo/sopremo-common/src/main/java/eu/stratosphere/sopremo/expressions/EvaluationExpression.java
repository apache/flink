package eu.stratosphere.sopremo.expressions;

import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.util.IdentitySet;

public abstract class EvaluationExpression extends SopremoExpression<EvaluationContext> {
	private static final class SameValueExpression extends EvaluationExpression implements WritableEvaluable {
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
		public JsonNode set(JsonNode node, JsonNode value, EvaluationContext context) {
			return value;
		}

		@Override
		public EvaluationExpression asExpression() {
			return this;
		}

		@Override
		protected void toString(final StringBuilder builder) {
			builder.append("<value>");
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1226647739750484403L;

	/**
	 * Used for secondary information during plan creation only.
	 */
	private transient Set<ExpressionTag> tags = new IdentitySet<ExpressionTag>();
	
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
		protected void toString(final StringBuilder builder) {
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
		protected void toString(final StringBuilder builder) {
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

	public EvaluationExpression withTag(final ExpressionTag tag) {
		this.addTag(tag);
		return this;
	}
}
