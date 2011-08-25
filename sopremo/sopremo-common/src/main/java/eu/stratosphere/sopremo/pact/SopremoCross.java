package eu.stratosphere.sopremo.pact;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.sopremo.EvaluationContext;

public abstract class SopremoCross<IK1 extends PactJsonObject.Key, IV1 extends PactJsonObject, IK2 extends PactJsonObject.Key, IV2 extends PactJsonObject, OK extends Key, OV extends PactJsonObject>
		extends
		CrossStub<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
	private EvaluationContext context;

	@Override
	public void configure(final Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
		this.context.setTaskId(parameters.getInteger(AbstractTask.TASK_ID, 0));
		SopremoUtil.configureStub(this, parameters);
	}

	protected abstract void cross(JsonNode key1, JsonNode value1, JsonNode key2, JsonNode value2, JsonCollector out);

	@Override
	public void cross(final PactJsonObject.Key key1, final PactJsonObject value1, final PactJsonObject.Key key2,
			final PactJsonObject value2,
			final Collector<PactJsonObject.Key, PactJsonObject> out) {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format("%s %s/%s %s/%s", getContext().operatorTrace(), key1, value1, key2,
				value2));
		this.cross(key1.getValue(), value1.getValue(), key2.getValue(), value2.getValue(), new JsonCollector(out));
	}

	protected EvaluationContext getContext() {
		return this.context;
	}
}
