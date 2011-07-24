package eu.stratosphere.sopremo.pact;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.sopremo.EvaluationContext;

public abstract class SopremoCross<IK1 extends PactJsonObject.Key, IV1 extends PactJsonObject, IK2 extends PactJsonObject.Key, IV2 extends PactJsonObject, OK extends Key, OV extends PactJsonObject>
		extends
		CrossStub<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	@Override
	public void cross(PactJsonObject.Key key1, PactJsonObject value1, PactJsonObject.Key key2, PactJsonObject value2,
			Collector<PactJsonObject.Key, PactJsonObject> out) {
		context.increaseInputCounter();
		if (SopremoUtil.LOG.isDebugEnabled())
			SopremoUtil.LOG.debug(String.format("%s %s/%s %s/%s", getClass().getSimpleName(), key1, value1, key2,
				value2));
		cross(key1.getValue(), value1.getValue(), key2.getValue(), value2.getValue(), new JsonCollector(out));
	}

	protected abstract void cross(JsonNode key1, JsonNode value1, JsonNode key2, JsonNode value2, JsonCollector out);
}
