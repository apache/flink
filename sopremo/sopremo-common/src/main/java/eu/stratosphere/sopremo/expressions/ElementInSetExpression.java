package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.tree.ChildIterator;
import eu.stratosphere.sopremo.expressions.tree.NamedChildIterator;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Determines a set contains an element or not.
 */
@OptimizerHints(scope = Scope.ANY, iterating = true)
public class ElementInSetExpression extends BinaryBooleanExpression implements ExpressionParent {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2695263646399347776L;

	private EvaluationExpression elementExpr;

	private EvaluationExpression setExpr;;

	private final Quantor quantor;

	/**
	 * Initializes an ElementInSetExpression.
	 * 
	 * @param elementExpr
	 *        the expression which evaluates to the element that should be found
	 * @param quantor
	 *        the {@link Quantor} that should be used
	 * @param setExpr
	 *        the expression which evaluates to the set that should be used
	 */
	public ElementInSetExpression(final EvaluationExpression elementExpr, final Quantor quantor,
			final EvaluationExpression setExpr) {
		this.elementExpr = elementExpr;
		this.setExpr = setExpr;
		this.quantor = quantor;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		// we can ignore 'target' because no new Object is created
		return this.quantor.evaluate(this.elementExpr.evaluate(node, null, context),
			ElementInSetExpression.asIterator(this.setExpr.evaluate(node, null, context)));
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ExpressionParent#iterator()
	 */
	@Override
	public ChildIterator iterator() {
		return new NamedChildIterator("elementExpr", "setExpr") {

			@Override
			protected void set(int index, EvaluationExpression childExpression) {
				if (index == 0)
					ElementInSetExpression.this.elementExpr = childExpression;
				else
					ElementInSetExpression.this.setExpr = childExpression;
			}

			@Override
			protected EvaluationExpression get(int index) {
				if (index == 0)
					return ElementInSetExpression.this.elementExpr;
				return ElementInSetExpression.this.setExpr;
			}
		};
	}

	// @Override
	// public Iterator<IJsonNode> evaluate(Iterator<IJsonNode>... inputs) {
	// return new AbstractIterator<IJsonNode>() {
	// @Override
	// protected IJsonNode loadNext() {
	// return isIn(elementExpr.evaluate(inputs[0].e).next(), setExpr.evaluate(inputs)) != notIn ? BooleanNode.TRUE
	// : BooleanNode.FALSE;
	// ;
	// }
	// };
	//
	// }
	//
	// @Override
	// public Iterator<IJsonNode> evaluate(Iterator<IJsonNode> input) {
	// return super.evaluate(input);
	// }

	/**
	 * Returns the element expression.
	 * 
	 * @return the element expression
	 */
	public EvaluationExpression getElementExpr() {
		return this.elementExpr;
	}

	/**
	 * Returns the quantor.
	 * 
	 * @return the quantor
	 */
	public Quantor getQuantor() {
		return this.quantor;
	}

	/**
	 * Returns the set expression.
	 * 
	 * @return the set expression
	 */
	public EvaluationExpression getSetExpr() {
		return this.setExpr;
	}

	@Override
	public void toString(final StringBuilder builder) {
		builder.append(this.elementExpr).append(this.quantor == Quantor.EXISTS_NOT_IN ? " \u2209 " : " \u2208 ")
			.append(this.setExpr);
	}

	//
	// @Override
	// public IJsonNode evaluate(IJsonNode... nodes) {
	// return quantor.evaluate(this.elementExpr.evaluate(nodes), this.asIterator(this.setExpr.evaluate(nodes)));
	// }

	static Iterator<IJsonNode> asIterator(final IJsonNode evaluate) {
		if (evaluate instanceof ArrayNode)
			return ((ArrayNode) evaluate).iterator();
		return Arrays.asList(evaluate).iterator();
	}

	/**
	 * All supported quantors.
	 */
	public static enum Quantor {
		EXISTS_IN, EXISTS_NOT_IN {
			@Override
			protected BooleanNode evaluate(final IJsonNode element, final Iterator<IJsonNode> set) {
				return super.evaluate(element, set) == BooleanNode.TRUE ? BooleanNode.FALSE : BooleanNode.TRUE;
			}
		};

		protected BooleanNode evaluate(final IJsonNode element, final Iterator<IJsonNode> set) {
			while (set.hasNext())
				if (element.equals(set.next()))
					return BooleanNode.TRUE;
			return BooleanNode.FALSE;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.elementExpr.hashCode();
		result = prime * result + this.quantor.hashCode();
		result = prime * result + this.setExpr.hashCode();
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final ElementInSetExpression other = (ElementInSetExpression) obj;
		return this.quantor == other.quantor
			&& this.elementExpr.equals(other.elementExpr)
			&& this.setExpr.equals(other.setExpr);
	}

}
