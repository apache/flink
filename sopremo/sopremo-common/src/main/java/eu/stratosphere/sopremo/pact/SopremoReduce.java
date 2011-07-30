package eu.stratosphere.sopremo.pact;

import java.util.ArrayList;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.StreamArrayNode;

public abstract class SopremoReduce<IK extends PactJsonObject.Key, IV extends PactJsonObject, OK extends PactJsonObject.Key, OV extends PactJsonObject>
		extends ReduceStub<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
		SopremoUtil.configureStub(this, parameters);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected abstract void reduce(JsonNode key, StreamArrayNode values, JsonCollector out);

	@Override
	public void reduce(PactJsonObject.Key key, Iterator<PactJsonObject> values,
			Collector<PactJsonObject.Key, PactJsonObject> out) {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isDebugEnabled()) {
			ArrayList<PactJsonObject> cached = new ArrayList<PactJsonObject>();
			while (values.hasNext())
				cached.add(values.next());
			SopremoUtil.LOG.debug(String.format("%s %s/%s", this.getClass().getSimpleName(), key, cached));
			values = cached.iterator();
		}
		this.reduce(key.getValue(), JsonUtil.wrapWithNode(this.needsResettableIterator(key, values), values), new JsonCollector(
			out));
	}

	protected boolean needsResettableIterator(PactJsonObject.Key key, Iterator<PactJsonObject> values) {
		return false;
	}
}
