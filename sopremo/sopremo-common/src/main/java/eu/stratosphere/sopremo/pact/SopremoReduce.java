package eu.stratosphere.sopremo.pact;

import java.util.ArrayList;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.StreamArrayNode;

public abstract class SopremoReduce<IK extends PactJsonObject.Key, IV extends PactJsonObject, OK extends PactJsonObject.Key, OV extends PactJsonObject>
		extends ReduceStub<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
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

	protected boolean needsResettableIterator(final PactJsonObject.Key key, final Iterator<PactJsonObject> values) {
		return false;
	}

	protected abstract void reduce(JsonNode key, StreamArrayNode values, JsonCollector out);

	@Override
	public void reduce(final PactJsonObject.Key key, Iterator<PactJsonObject> values,
			final Collector<PactJsonObject.Key, PactJsonObject> out) {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isTraceEnabled()) {
			final ArrayList<PactJsonObject> cached = new ArrayList<PactJsonObject>();
			while (values.hasNext())
				cached.add(values.next());
			SopremoUtil.LOG.trace(String.format("%s %s/%s", getContext().operatorTrace(), key, cached));
			values = cached.iterator();
		}
		this.reduce(key.getValue(), JsonUtil.wrapWithNode(this.needsResettableIterator(key, values), values),
			new JsonCollector(
				out));
	}
}
