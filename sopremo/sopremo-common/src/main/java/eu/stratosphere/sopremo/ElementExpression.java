package eu.stratosphere.sopremo;

import java.util.Arrays;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.sopremo.expressions.EvaluableExpression;

public class ElementExpression extends BooleanExpression {
	private EvaluableExpression elementExpr, setExpr;

	private boolean notIn;

	public ElementExpression(EvaluableExpression elementExpr, EvaluableExpression setExpr, boolean notIn) {
		this.elementExpr = elementExpr;
		this.setExpr = setExpr;
		this.notIn = notIn;
	}

	public ElementExpression(EvaluableExpression elementExpr, EvaluableExpression setExpr) {
		this(elementExpr, setExpr, false);
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
		return elementExpr;
	}

	public EvaluableExpression getSetExpr() {
		return setExpr;
	}

	public boolean isNotIn() {
		return notIn;
	}

	@Override
	public JsonNode evaluate(JsonNode node) {
		return this.isIn(this.elementExpr.evaluate(node), this.asIterator(this.setExpr.evaluate(node))) != this.notIn ? BooleanNode.TRUE
			: BooleanNode.FALSE;
	}

	@Override
	public JsonNode evaluate(JsonNode... nodes) {
		return this.isIn(this.elementExpr.evaluate(nodes), this.asIterator(this.setExpr.evaluate(nodes))) != this.notIn ? BooleanNode.TRUE
			: BooleanNode.FALSE;
	}

	private Iterator<JsonNode> asIterator(JsonNode evaluate) {
		if (evaluate instanceof ArrayNode)
			return ((ArrayNode) evaluate).iterator();
		return Arrays.asList(evaluate).iterator();
	}

	private boolean isIn(JsonNode element, Iterator<JsonNode> set) {
		while (set.hasNext())
			if (this.asIterator(element).equals(set.next()))
				return true;
		return false;
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append(this.elementExpr).append(this.notIn ? " \u2209 " : " \u2208 ").append(this.setExpr);
	}
}
