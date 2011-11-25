package eu.stratosphere.sopremo.cleansing.fusion;

import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.NullNode;

public class MostFrequentRule extends ConflictResolution {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8375051813752883648L;

	private final Object2DoubleMap<JsonNode> histogram = new Object2DoubleOpenHashMap<JsonNode>();

	@Override
	public JsonNode fuse(final JsonNode[] values, final double[] weights, final FusionContext context) {
		this.histogram.clear();
		for (int index = 0; index < values.length; index++)
			this.histogram.put(values[index], this.histogram.get(values[index]) + weights[index]);

		final ObjectSet<Object2DoubleMap.Entry<JsonNode>> entrySet = this.histogram.object2DoubleEntrySet();
		double max = 0;
		JsonNode maxObject = NullNode.getInstance();
		for (final Object2DoubleMap.Entry<JsonNode> entry : entrySet)
			if (entry.getDoubleValue() > max) {
				max = entry.getDoubleValue();
				maxObject = entry.getKey();
			}
		return maxObject;
	}
}
