package eu.stratosphere.sopremo.pact;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;

public abstract class SopremoCoGroup<IK extends PactJsonObject.Key, IV1 extends PactJsonObject, IV2 extends PactJsonObject, OK extends PactJsonObject.Key, OV extends PactJsonObject>
		extends CoGroupStub<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected abstract void coGroup(JsonNode key, StreamArrayNode values1, StreamArrayNode values2, JsonCollector out);

	@Override
	public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
			Collector<PactJsonObject.Key, PactJsonObject> out) {
		coGroup(key.getValue(), JsonUtil.wrapWithNode(true, values1), JsonUtil.wrapWithNode(true, values2),
			new JsonCollector(out));
	}
}
