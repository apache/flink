package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.Schema;

public abstract class SopremoMap extends MapStub {
	private EvaluationContext context;

	private Schema inputSchema;

	private JsonCollector collector;

	private JsonNode cachedInput;

	@Override
	public void open(final Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, SopremoUtil.CONTEXT, EvaluationContext.class);
		this.inputSchema = this.context.getInputSchema(0);
		this.collector = new JsonCollector(this.context.getOutputSchema(0));
		SopremoUtil.configureStub(this, parameters);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected abstract void map(JsonNode value, JsonCollector out);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord,
	 * eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void map(final PactRecord record, final Collector out) throws Exception {
		this.context.increaseInputCounter();
		this.collector.setCollector(out);
		final JsonNode input = this.inputSchema.recordToJson(record, this.cachedInput);
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format("%s %s", this.getContext().operatorTrace(), input));
		try {
			this.map(input, this.collector);
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with %s: %s", this.getContext().operatorTrace(),
				this.cachedInput, e));
			throw e;
		}
	};
}
