package eu.stratosphere.sopremo.pact;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.sopremo.type.JsonNode;

public class JsonCollector {
	private final Collector collector;

	public JsonCollector(final Collector collector) {
		this.collector = collector;
	}

	public void collect(final JsonNode value) {
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format(" to %s", value));
		this.collector.collect(SopremoUtil.jsonToRecord(value));
	}
}
