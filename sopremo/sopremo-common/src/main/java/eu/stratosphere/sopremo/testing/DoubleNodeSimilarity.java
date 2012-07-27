package eu.stratosphere.sopremo.testing;

import java.util.Iterator;
import java.util.Map.Entry;

import eu.stratosphere.pact.testing.AbstractValueSimilarity;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IJsonNode.Type;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IObjectNode;

public class DoubleNodeSimilarity extends AbstractValueSimilarity<JsonNodeWrapper> {
	private final double delta;

	/**
	 * Initializes DoubleNodeSimilarity with the given threshold.
	 * 
	 * @param delta
	 *        the threshold defining the maximum allowed difference.
	 */
	public DoubleNodeSimilarity(final double delta) {
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
	public double getDistance(final JsonNodeWrapper value1, final JsonNodeWrapper value2) {
		return this.getDistance(value1.getValue(), value2.getValue());
	}

	public double getDistance(final IJsonNode value1, final IJsonNode value2) {
		if (value1.equals(value2))
			return 0;

		final Type type1 = value1.getType();
		final Type type2 = value2.getType();
		if (type1 == type2)
			if (type1 == Type.ArrayNode)
				return this.getDistance((IArrayNode) value1, (IArrayNode) value2);
			else if (type1 == Type.ObjectNode)
				return this.getDistance((IObjectNode) value1, (IObjectNode) value2);
		if (!type1.isNumeric() || !type2.isNumeric())
			return NO_MATCH;

		final INumericNode inner1 = (INumericNode) value1;
		final INumericNode inner2 = (INumericNode) value2;
		// at this point, both values are double
		final double diff = Math.abs(inner1.getDoubleValue() - inner2.getDoubleValue());
		return diff <= this.delta ? diff : NO_MATCH;
	}

	private double getDistance(final IArrayNode value1, final IArrayNode value2) {
		double distance = 0;
		for (int index = 0; index < Math.max(value1.size(), value2.size()); index++) {
			final double elemDistance = this.getDistance(value1.get(index), value2.get(index));
			if (elemDistance < 0)
				return elemDistance;
			distance += elemDistance;
		}
		return distance;
	}

	private double getDistance(final IObjectNode value1, final IObjectNode value2) {
		double distance = 0;
		final Iterator<Entry<String, IJsonNode>> entries1 = value1.iterator(), entries2 = value2.iterator();

		while (entries1.hasNext() && entries2.hasNext()) {
			final Entry<String, IJsonNode> entry1 = entries1.next(), entry2 = entries2.next();
			if (!entry1.getKey().equals(entry2.getKey()))
				return NO_MATCH;

			final double valueComparison = this.getDistance(entry1.getValue(), entry2.getValue());
			if (valueComparison < 0)
				return NO_MATCH;
			distance += valueComparison;
		}

		if (entries1.hasNext() || entries2.hasNext())
			return NO_MATCH;
		return distance;
	}
}
