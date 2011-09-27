package eu.stratosphere.sopremo.pact;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class JsonCollector {
	private final Collector<JsonNode, JsonNode> collector;

	public JsonCollector(final Collector<JsonNode, JsonNode> collector) {
		this.collector = collector;
	}

	public void collect(final JsonNode key, final JsonNode value) {
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format(" to %s/%s", key, value));
		this.collector.collect(key, value);
	}
}
