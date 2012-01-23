package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.JsonNode;

public abstract class SopremoMap extends MapStub {
	private EvaluationContext context;

	@Override
	public void open(final Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
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
	public void map(PactRecord record, Collector out) throws Exception {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format("%s %s/%s", this.getContext().operatorTrace(), key, value));
		try {
			this.map(SopremoUtil.wrap(record), new JsonCollector(out));
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with k/v %s/%s: %s", this.getContext()
				.operatorTrace(), key, value, e));
			throw e;
		}
	};
}
