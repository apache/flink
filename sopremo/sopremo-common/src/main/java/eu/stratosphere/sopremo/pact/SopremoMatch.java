package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.JsonNode;

public abstract class SopremoMatch extends MatchStub {
	private EvaluationContext context;

	@Override
	public void configure(final Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
		SopremoUtil.configureStub(this, parameters);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected abstract void match(JsonNode key, JsonNode value1, JsonNode value2, JsonCollector out);

	@Override
	public void match(final JsonNode key, final JsonNode value1, final JsonNode value2,
			final Collector<JsonNode, JsonNode> out) {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format("%s %s/%s/%s", this.getContext().operatorTrace(), key, value1, value2));
		try {
			this.match(SopremoUtil.unwrap(key), SopremoUtil.unwrap(value1), SopremoUtil.unwrap(value2),
				new JsonCollector(out));
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with k/v/v %s/%s/%s: %s", this.getContext()
				.operatorTrace(), key, value1, value2, e));
			throw e;
		}
	}
}
