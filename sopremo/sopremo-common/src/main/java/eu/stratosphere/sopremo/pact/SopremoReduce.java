package eu.stratosphere.sopremo.pact;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;

public abstract class SopremoReduce<IK extends PactJsonObject.Key, IV extends PactJsonObject, OK extends PactJsonObject.Key, OV extends PactJsonObject>
		extends ReduceStub<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected abstract void reduce(JsonNode key1, JsonNode values, JsonCollector out);

	@Override
	public void reduce(PactJsonObject.Key key, Iterator<PactJsonObject> values,
			Collector<PactJsonObject.Key, PactJsonObject> out) {
		reduce(key.getValue(), JsonUtil.wrapWithNode(true, values), new JsonCollector(out));
	}
}
