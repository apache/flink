package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public abstract class SopremoMap<IK extends JsonNode, IV extends JsonNode, OK extends JsonNode, OV extends JsonNode>
		extends MapStub<JsonNode, JsonNode, JsonNode, JsonNode> {
	private EvaluationContext context;

	@Override
	public void configure(final Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
		this.context.setTaskId(parameters.getInteger(AbstractTask.TASK_ID, 0));
		SopremoUtil.configureStub(this, parameters);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected abstract void map(JsonNode key, JsonNode value, JsonCollector out);

	@Override
	public void map(final JsonNode key, final JsonNode value,
			final Collector<JsonNode, JsonNode> out) {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format("%s %s/%s", this.getContext().operatorTrace(), key, value));
		try {
			this.map(key, value, new JsonCollector(out));
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with k/v %s/%s: %s", this.getContext()
				.operatorTrace(), key, value, e));
			throw e;
		}
	};
}
