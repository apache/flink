package eu.stratosphere.sopremo.pact;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonNode;

public class JsonCollector {
	private Collector collector;

	private final Schema schema;

	private PactRecord record;

	public JsonCollector(final Schema schema) {
		this.schema = schema;
	}

	/**
	 * Sets the collector to the specified value.
	 * 
	 * @param collector
	 *        the collector to set
	 */
	public void setCollector(final Collector collector) {
		this.collector = collector;
	}

	public void collect(final IJsonNode value) {
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format(" to %s", value));
		this.collector.collect(this.record = this.schema.jsonToRecord(value, null, null));
	}
}
