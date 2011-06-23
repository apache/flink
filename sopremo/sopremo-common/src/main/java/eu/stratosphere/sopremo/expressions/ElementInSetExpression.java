package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.EvaluationContext;

@OptimizerHints(scope = Scope.ANY, iterating = true)
public class ElementInSetExpression extends BooleanExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2695263646399347776L;

	private EvaluableExpression elementExpr, setExpr;;

	private Quantor quantor;

	public ElementInSetExpression(EvaluableExpression elementExpr, Quantor quantor, EvaluableExpression setExpr) {
		this.elementExpr = elementExpr;
		this.setExpr = setExpr;
		this.quantor = quantor;
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		return this.quantor.evaluate(this.elementExpr.evaluate(node, context),
			ElementInSetExpression.asIterator(this.setExpr.evaluate(node, context)));
	}

	// @Override
	// public Iterator<JsonNode> evaluate(Iterator<JsonNode>... inputs) {
	// return new AbstractIterator<JsonNode>() {
	// @Override
	// protected JsonNode loadNext() {
	// return isIn(elementExpr.evaluate(inputs[0].e).next(), setExpr.evaluate(inputs)) != notIn ? BooleanNode.TRUE
	// : BooleanNode.FALSE;
	// ;
	// }
	// };
	//
	// }
	//
	// @Override
	// public Iterator<JsonNode> evaluate(Iterator<JsonNode> input) {
	// return super.evaluate(input);
	// }

	public EvaluableExpression getElementExpr() {
		return this.elementExpr;
	}

	public Quantor getQuantor() {
		return this.quantor;
	}

	public EvaluableExpression getSetExpr() {
		return this.setExpr;
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(this.elementExpr).append(this.quantor == Quantor.EXISTS_NOT_IN ? " \u2209 " : " \u2208 ")
			.append(this.setExpr);
	}

	//
	// @Override
	// public JsonNode evaluate(JsonNode... nodes) {
	// return quantor.evaluate(this.elementExpr.evaluate(nodes), this.asIterator(this.setExpr.evaluate(nodes)));
	// }

	static Iterator<JsonNode> asIterator(JsonNode evaluate) {
		if (evaluate instanceof ArrayNode)
			return ((ArrayNode) evaluate).iterator();
		return Arrays.asList(evaluate).iterator();
	}

	public static enum Quantor {
		EXISTS_IN, EXISTS_NOT_IN {
			@Override
			protected BooleanNode evaluate(JsonNode element, Iterator<JsonNode> set) {
				return super.evaluate(element, set) == BooleanNode.TRUE ? BooleanNode.FALSE : BooleanNode.TRUE;
			}
		};

		protected BooleanNode evaluate(JsonNode element, Iterator<JsonNode> set) {
			while (set.hasNext())
				if (asIterator(element).equals(set.next()))
					return BooleanNode.TRUE;
			return BooleanNode.FALSE;
		}
	}
}
