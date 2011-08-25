package eu.stratosphere.sopremo.pact;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;

public abstract class SopremoMatch<IK extends PactJsonObject.Key, IV1 extends PactJsonObject, IV2 extends PactJsonObject, OK extends PactJsonObject.Key, OV extends PactJsonObject>
		extends MatchStub<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
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

	protected abstract void match(JsonNode key, JsonNode value1, JsonNode value2, JsonCollector out);

	@Override
	public void match(final PactJsonObject.Key key, final PactJsonObject value1, final PactJsonObject value2,
			final Collector<Key, PactJsonObject> out) {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format("%s %s/%s/%s", getContext().operatorTrace(), key, value1, value2));
		this.match(key.getValue(), value1.getValue(), value2.getValue(), new JsonCollector(out));
	}
}
