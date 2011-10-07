package eu.stratosphere.sopremo.base;

import java.util.Iterator;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;

/**
 * Splits an object into multiple outgoing tuples.<br>
 * This operator provides a means to emit more than one tuple in contrast to most other base operators.
 * 
 * @author Arvid Heise
 */
public class ObjectSplit extends ElementaryOperator<ObjectSplit> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2967507260239105002L;

	private EvaluationExpression objectPath = EvaluationExpression.VALUE, keyProjection = new ArrayAccess(1),
			valueProjection = new ArrayAccess(0);

	public EvaluationExpression getObjectPath() {
		return this.objectPath;
	}

	public EvaluationExpression getValueProjection() {
		return this.valueProjection;
	}

	/**
	 * (element, index/fieldName, array/object, node) -&gt; value
	 * 
	 * @param valueProjection
	 * @return this
	 */
	public ObjectSplit withValueProjection(EvaluationExpression valueProjection) {
		this.setValueProjection(valueProjection);
		return this;
	}

	/**
	 * (element, index/fieldName, array/object, node) -&gt; value
	 * 
	 * @param valueProjection
	 */
	public void setValueProjection(EvaluationExpression valueProjection) {
		this.valueProjection = valueProjection;
	}

	/**
	 * (element, index/fieldName, array/object, node) -&gt; key
	 * 
	 * @param keyProjection
	 * @return
	 */
	public ObjectSplit withKeyProjection(EvaluationExpression keyProjection) {
		this.keyProjection = keyProjection;
		return this;
	}

	public EvaluationExpression getKeyProjection() {
		return this.keyProjection;
	}

	public ObjectSplit setObjectProjection(EvaluationExpression objectPath) {
		this.objectPath = objectPath;
		return this;
	}
	
	public ObjectSplit withObjectProjection(EvaluationExpression objectProjection) {
		setObjectProjection(objectProjection);
		return this;
	}

	public static class Implementation extends SopremoMap<JsonNode, JsonNode, JsonNode, JsonNode> {
		private EvaluationExpression objectPath, valueProjection, keyProjection;

		@Override
		protected void map(JsonNode key, JsonNode value, JsonCollector out) {
			final JsonNode object = this.objectPath.evaluate(value, this.getContext());
			if (!object.isObject())
				throw new EvaluationException("Cannot split non-object");

			final Iterator<String> fieldNames = ((ObjectNode) object).getFieldNames();
			final EvaluationContext context = this.getContext();
			while (fieldNames.hasNext()) {
				String field = fieldNames.next();
				final TextNode fieldNode = TextNode.valueOf(field);
				out.collect(
					this.keyProjection.evaluate(JsonUtil.asArray(((ObjectNode) value).get(field), fieldNode, object, value), context),
					this.valueProjection.evaluate(JsonUtil.asArray(((ObjectNode) value).get(field), fieldNode, object, value), context));
			}
		}
	}
}
