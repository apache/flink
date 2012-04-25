package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * Splits an array into multiple tuples.<br>
 * This operator provides a means to emit more than one tuple in contrast to most other base operators.
 * 
 * @author Arvid Heise
 */
public class ArraySplit extends ElementaryOperator<ArraySplit> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2967507260239105002L;

	private EvaluationExpression arrayPath = EvaluationExpression.VALUE, elementProjection = new ArrayAccess(0);

	public EvaluationExpression getArrayPath() {
		return this.arrayPath;
	}

	public EvaluationExpression getElementProjection() {
		return this.elementProjection;
	}

	public ArraySplit withArrayPath(EvaluationExpression arrayPath) {
		this.arrayPath = arrayPath;
		return this;
	}

	/**
	 * (element, index, array, node) -&gt; value
	 * 
	 * @param valueProjection
	 * @return this
	 */
	public ArraySplit withValueProjection(EvaluationExpression valueProjection) {
		this.setElementProjection(valueProjection);
		return this;
	}

	/**
	 * (element, index, array, node) -&gt; value
	 * 
	 * @param valueProjection
	 */
	public void setElementProjection(EvaluationExpression valueProjection) {
		this.elementProjection = valueProjection;
	}

	public static class Implementation extends SopremoMap {
		private EvaluationExpression arrayPath, elementProjection;

		@Override
		protected void map(final IJsonNode value, JsonCollector out) {
			final IJsonNode array = this.arrayPath.evaluate(value, this.getContext());
			if (!array.isArray())
				throw new EvaluationException("Cannot split non-array");

			int index = 0;
			final EvaluationContext context = this.getContext();
			IntNode indexNode = IntNode.valueOf(0);
			ArrayNode contextNode = JsonUtil.asArray(NullNode.getInstance(), indexNode, array, value);
			for (IJsonNode element : (IArrayNode) array) {
				contextNode.set(0, element);
				indexNode.setValue(index++);
				out.collect(this.elementProjection.evaluate(contextNode, context));
			}
		}
	}
}
