package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public abstract class SopremoMatch<IK extends JsonNode, IV1 extends JsonNode, IV2 extends JsonNode, OK extends JsonNode, OV extends JsonNode>
		extends MatchStub<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {
	private EvaluationContext context;

	@Override
	public Class<JsonNode> getFirstInKeyType() {
		return SopremoUtil.WRAPPER_TYPE;
	}

	@Override
	public Class<JsonNode> getSecondInKeyType() {
		return SopremoUtil.WRAPPER_TYPE;
	}

	@Override
	public Class<JsonNode> getFirstInValueType() {
		return SopremoUtil.WRAPPER_TYPE;
	}

	@Override
	public Class<JsonNode> getSecondInValueType() {
		return SopremoUtil.WRAPPER_TYPE;
	}

	@Override
	public Class<JsonNode> getOutKeyType() {
		return SopremoUtil.WRAPPER_TYPE;
	}

	@Override
	public Class<JsonNode> getOutValueType() {
		return SopremoUtil.WRAPPER_TYPE;
	}

	@Override
	public void configure(final Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
		this.context.setTaskId(parameters.getInteger(AbstractTask.TASK_ID, 0));
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
