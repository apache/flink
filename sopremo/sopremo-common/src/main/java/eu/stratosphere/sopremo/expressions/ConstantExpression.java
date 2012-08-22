package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * Represents all constants.
 */
@OptimizerHints(scope = Scope.ANY)
public class ConstantExpression extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4270374147359826240L;

	private final IJsonNode constant;

	public static final EvaluationExpression MISSING = new ConstantExpression(MissingNode.getInstance()) {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2375203649638430872L;

		private Object readResolve() {
			return ConstantExpression.MISSING;
		}
	};

	public static final EvaluationExpression NULL = new ConstantExpression(NullNode.getInstance()) {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2375203649638430872L;

		private Object readResolve() {
			return ConstantExpression.NULL;
		}
	};

	/**
	 * Initializes a ConstantExpression with the given JsonNode.
	 * 
	 * @param constant
	 *        the node that should be represented by this ConstantExpression
	 */
	public ConstantExpression(final IJsonNode constant) {
		this.constant = constant;
	}

	/**
	 * Initializes a ConstantExpression. The given constant will be mapped to a JsonNode before initializing this
	 * expression.
	 * 
	 * @param constant
	 *        this Objects JsonNode representation should be represented by this ConstantExpression
	 */
	public ConstantExpression(final Object constant) {
		this.constant = JsonUtil.OBJECT_MAPPER.valueToTree(constant);
	}

	/**
	 * Returns the constant.
	 * 
	 * @return the constant
	 */
	public IJsonNode getConstant() {
		return this.constant;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ConstantExpression other = (ConstantExpression) obj;
		return this.constant.equals(other.constant);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		// we can ignore 'target' because no new Object is created
		return this.constant;
	}

	@Override
	public int hashCode() {
		return 41 * super.hashCode() + this.constant.hashCode();
	}

	@Override
	public void toString(final StringBuilder builder) {
		if (this.constant instanceof CharSequence)
			builder.append("\'").append(this.constant).append("\'");
		else
			builder.append(this.constant);
	}

}