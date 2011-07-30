package eu.stratosphere.sopremo.cleansing.conflict_resolution;

import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;
import eu.stratosphere.sopremo.aggregation.MaterializingAggregationFunction;

public class MostFrequentResolution extends AggregationFunction {
	public MostFrequentResolution() {
		super("most frequent");
	}

	private Object2IntMap<JsonNode> histogram;

	@Override
	public void initialize() {
		histogram = new Object2IntLinkedOpenHashMap<JsonNode>();
	}

	@Override
	public void aggregate(JsonNode node, EvaluationContext context) {
		histogram.put(node, 1 + histogram.getInt(node));
	}

	@Override
	public JsonNode getFinalAggregate() {
		ObjectSet<Entry<JsonNode>> entrySet = histogram.object2IntEntrySet();
		int max = 0;
		JsonNode maxObject = NullNode.getInstance();
		for (Entry<JsonNode> entry : entrySet)
			if (entry.getIntValue() > max) {
				max = entry.getIntValue();
				maxObject = entry.getKey();
			}
		return maxObject;
	}
}
