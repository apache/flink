package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * Splits an array into multiple tuples.<br>
 * This operator provides a means to emit more than one tuple in contrast to most other base operators.
 * 
 * @author Arvid Heise
 */
@InputCardinality(1)
public class ArraySplit extends ElementaryOperator<ArraySplit> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2967507260239105002L;

	private EvaluationExpression arrayPath = EvaluationExpression.VALUE, splitProjection = new ArrayAccess(0);

	public enum ResultField {
		Element, Index, Array, WholeValue;
	};

	public EvaluationExpression getArrayPath() {
		return this.arrayPath;
	}

	public EvaluationExpression getSplitProjection() {
		return this.splitProjection;
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
	public ArraySplit withSplitProjection(EvaluationExpression valueProjection) {
		this.setSplitProjection(valueProjection);
		return this;
	}

	/**
	 * (element, index, array, node) -&gt; value
	 * 
	 * @param elementProjection
	 */
	public void setSplitProjection(EvaluationExpression elementProjection) {
		if (elementProjection == null)
			throw new NullPointerException("elementProjection must not be null");
		this.splitProjection = elementProjection;
	}

	/**
	 * (element, index, array, node) -&gt; value
	 * 
	 * @param valueProjection
	 * @return this
	 */
	public ArraySplit withSplitProjection(ResultField... fields) {
		this.setSplitProjection(fields);
		return this;
	}

	/**
	 * (element, index, array, node) -&gt; value
	 * 
	 * @param valueProjection
	 */
	public void setSplitProjection(ResultField... fields) {
		int[] indices = new int[fields.length];
		for (int index = 0; index < indices.length; index++) 
			indices[index] = fields[index].ordinal();
		this.setSplitProjection(ArrayAccess.arrayWithIndices(indices));
	}

	public static class Implementation extends SopremoMap {
		private CachingExpression<IArrayNode> arrayPath;

		private EvaluationExpression splitProjection;

		@Override
		protected void map(final IJsonNode value, JsonCollector out) {
			final IArrayNode array = this.arrayPath.evaluate(value, null, this.getContext());
			if (!array.isArray())
				throw new EvaluationException("Cannot split non-array");

			int index = 0;
			final EvaluationContext context = this.getContext();
			IntNode indexNode = IntNode.valueOf(0);
			IArrayNode contextNode = JsonUtil.asArray(NullNode.getInstance(), indexNode, array, value);
			for (IJsonNode element : array) {
				contextNode.set(0, element);
				indexNode.setValue(index);
				out.collect(this.splitProjection.evaluate(contextNode, null, context));
				index++;
			}
		}
	}
}
