package eu.stratosphere.sopremo.base;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.IntNode;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoMap;

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

	private EvaluationExpression arrayPath = EvaluationExpression.VALUE, keyProjection = new ArrayAccess(1),
			valueProjection = new ArrayAccess(0);

	public EvaluationExpression getArrayPath() {
		return this.arrayPath;
	}

	public EvaluationExpression getValueProjection() {
		return this.valueProjection;
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
		this.setValueProjection(valueProjection);
		return this;
	}

	/**
	 * (element, index, array, node) -&gt; value
	 * 
	 * @param valueProjection
	 */
	public void setValueProjection(EvaluationExpression valueProjection) {
		this.valueProjection = valueProjection;
	}

	/**
	 * (element, index, array, node) -&gt; key
	 * 
	 * @param keyProjection
	 * @return
	 */
	public ArraySplit withKeyProjection(EvaluationExpression keyProjection) {
		this.keyProjection = keyProjection;
		return this;
	}

	public EvaluationExpression getKeyProjection() {
		return this.keyProjection;
	}

	public static class Implementation extends SopremoMap<Key, PactJsonObject, Key, PactJsonObject> {
		private EvaluationExpression arrayPath, valueProjection, keyProjection;

		@Override
		protected void map(JsonNode key, JsonNode value, JsonCollector out) {
			final JsonNode array = this.arrayPath.evaluate(value, this.getContext());
			if (!array.isArray())
				throw new EvaluationException("Cannot split non-array");

			int index = 0;
			final EvaluationContext context = this.getContext();
			for (JsonNode element : array) {
				JsonNode indexNode = IntNode.valueOf(index++);
				out.collect(
					this.keyProjection.evaluate(JsonUtil.asArray(element, indexNode, array, value), context),
					this.valueProjection.evaluate(JsonUtil.asArray(element, indexNode, array, value), context));
			}
		}
	}
}
