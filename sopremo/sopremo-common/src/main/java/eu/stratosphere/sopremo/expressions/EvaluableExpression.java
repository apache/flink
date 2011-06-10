package eu.stratosphere.sopremo.expressions;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SerializableSopremoType;

/**
 * Base class for all first-order expressions in the Sopremo language, such as arithmetic expressions, functions, and
 * path notations.
 * 
 * @author Arvid Heise
 */
public abstract class EvaluableExpression implements SerializableSopremoType, Evaluable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 39927024374509247L;

	/**
	 * Represents an unresolvable expression. The value most likely indicates a programming error and an evaluation
	 * attempt causes an exceptions.
	 */
	public static final EvaluableExpression UNKNOWN = new IdentifierAccess("?");

	/**
	 * Represents an expression that returns the input node without any modifications. The constant is mostly used for
	 * {@link Operator}s that do not perform any transformation to the input, such as a filter operator.
	 */
	public static final EvaluableExpression IDENTITY = new EvaluableExpression() {

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

		@Override
		protected void toString(StringBuilder builder) {
		};
	};

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
}