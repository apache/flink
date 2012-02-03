package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IJsonNode;

@OptimizerHints(scope = Scope.ANY, iterating = true)
public class ElementInSetExpression extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2695263646399347776L;

	private final EvaluationExpression elementExpr, setExpr;;

	private final Quantor quantor;

	public ElementInSetExpression(final EvaluationExpression elementExpr, final Quantor quantor,
			final EvaluationExpression setExpr) {
		this.elementExpr = elementExpr;
		this.setExpr = setExpr;
		this.quantor = quantor;
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, final EvaluationContext context) {
		return this.quantor.evaluate(this.elementExpr.evaluate(node, context),
			ElementInSetExpression.asIterator(this.setExpr.evaluate(node, context)));
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

	public EvaluationExpression getElementExpr() {
		return this.elementExpr;
	}

	public Quantor getQuantor() {
		return this.quantor;
	}

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
