package eu.stratosphere.sopremo.testing;

import java.util.Iterator;
import java.util.Map.Entry;

import eu.stratosphere.pact.testing.AbstractValueSimilarity;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.type.AbstractJsonNode.Type;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IObjectNode;

public class DoubleNodeSimilarity extends AbstractValueSimilarity<JsonNodeWrapper> {
	private double delta;

	/**
	 * Initializes DoubleNodeSimilarity with the given threshold.
	 * 
	 * @param delta
	 *        the threshold defining the maximum allowed difference.
	 */
	public DoubleNodeSimilarity(double delta) {
		this.delta = delta;
	}

	/**
	 * Returns the threshold.
	 * 
	 * @return the threshold
	 */
	public double getDelta() {
		return this.delta;
	}

	@Override
	public double getDistance(JsonNodeWrapper value1, JsonNodeWrapper value2) {
		return getDistance(value1.getValue(), value2.getValue());
	}

	public double getDistance(IJsonNode value1, IJsonNode value2) {
		if (value1.equals(value2))
			return 0;

		final Type type1 = value1.getType();
		final Type type2 = value2.getType();
		if (type1 == type2) {
			if (type1 == Type.ArrayNode)
				return getDistance((IArrayNode) value1, (IArrayNode) value2);
			else if (type1 == Type.ObjectNode)
				return getDistance((IObjectNode) value1, (IObjectNode) value2);
		}
		if (!type1.isNumeric() || !type2.isNumeric())
			return NO_MATCH;

		final INumericNode inner1 = (INumericNode) value1;
		final INumericNode inner2 = (INumericNode) value2;
		// at this point, both values are double
		double diff = Math.abs(((INumericNode) inner1).getDoubleValue() - ((INumericNode) inner2).getDoubleValue());
		return diff <= this.delta ? diff : NO_MATCH;
	}

	private double getDistance(IArrayNode value1, IArrayNode value2) {
		double distance = 0;
		for (int index = 0; index < Math.max(value1.size(), value2.size()); index++) {
			double elemDistance = getDistance(value1.get(index), value2.get(index));
			if (elemDistance < 0)
				return elemDistance;
			distance += elemDistance;
		}
		return distance;
	}

	private double getDistance(IObjectNode value1, IObjectNode value2) {
		double distance = 0;
		final Iterator<Entry<String, IJsonNode>> entries1 = value1.iterator(), entries2 = value2.iterator();

		while (entries1.hasNext() && entries2.hasNext()) {
			final Entry<String, IJsonNode> entry1 = entries1.next(), entry2 = entries2.next();
			if (!entry1.getKey().equals(entry2.getKey()))
				return NO_MATCH;

			final double valueComparison = getDistance(entry1.getValue(), entry2.getValue());
			if (valueComparison < 0)
				return NO_MATCH;
			distance += valueComparison;
		}

		if (!entries1.hasNext()||!entries2.hasNext())
			return NO_MATCH;
		return distance;
	}
}
