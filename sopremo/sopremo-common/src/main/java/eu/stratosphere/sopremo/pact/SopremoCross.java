package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public abstract class SopremoCross<IK1 extends JsonNode, IV1 extends JsonNode, IK2 extends JsonNode, IV2 extends JsonNode, OK extends Key, OV extends JsonNode>
		extends
		CrossStub<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {
	private EvaluationContext context;

	@Override
	public void configure(final Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
		this.context.setTaskId(parameters.getInteger(AbstractTask.TASK_ID, 0));
		SopremoUtil.configureStub(this, parameters);
	}

	protected abstract void cross(JsonNode key1, JsonNode value1, JsonNode key2, JsonNode value2, JsonCollector out);

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
	public void cross(final JsonNode key1, final JsonNode value1, final JsonNode key2,
			final JsonNode value2,
			final Collector<JsonNode, JsonNode> out) {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format("%s %s/%s %s/%s", this.getContext().operatorTrace(), key1, value1,
				key2,
				value2));
		try {
			this.cross(SopremoUtil.unwrap(key1), SopremoUtil.unwrap(value1), SopremoUtil.unwrap(key2),
				SopremoUtil.unwrap(value2), new JsonCollector(out));
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with k1/v1 %s/%s k2/v2: %s", this.getContext()
				.operatorTrace(), key1, value1, key2, value2, e));
			throw e;
		}
	}

	protected EvaluationContext getContext() {
		return this.context;
	}
}
