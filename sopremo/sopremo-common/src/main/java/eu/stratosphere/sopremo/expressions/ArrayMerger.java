package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;

public class ArrayMerger extends EvaluableExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6884623565349727369L;

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		Iterator<JsonNode> arrays = node.getElements();
		ArrayNode mergedArray = JsonUtil.NODE_FACTORY.arrayNode();
		while (arrays.hasNext()) {
			JsonNode array = arrays.next();
			for (int index = 0; index < array.size(); index++)
				if (mergedArray.size() <= index)
					mergedArray.add(array.get(index));
				else if (this.isNull(mergedArray.get(index)) && !this.isNull(array.get(index)))
					mergedArray.set(index, array.get(index));
		}
		return mergedArray;
	}

	private boolean isNull(JsonNode value) {
		return value == null || value.isNull();
	}

	@Override
	protected void toString(StringBuilder builder) {
		builder.append("[*]+...+[*]");
	}

}
