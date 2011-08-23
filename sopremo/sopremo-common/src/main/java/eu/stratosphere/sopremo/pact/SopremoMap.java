package eu.stratosphere.sopremo.pact;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.sopremo.EvaluationContext;

public abstract class SopremoMap<IK extends PactJsonObject.Key, IV extends PactJsonObject, OK extends PactJsonObject.Key, OV extends PactJsonObject>
		extends MapStub<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
	private EvaluationContext context;

	@Override
	public void configure(final Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
		SopremoUtil.configureStub(this, parameters);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected abstract void map(JsonNode key, JsonNode value, JsonCollector out);

	@Override
	public void map(final PactJsonObject.Key key, final PactJsonObject value,
			final Collector<PactJsonObject.Key, PactJsonObject> out) {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format("%s %s/%s", getContext().operatorTrace(), key, value));
		this.map(key.getValue(), value.getValue(), new JsonCollector(out));
	};
}
