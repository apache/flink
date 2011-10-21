package eu.stratosphere.sopremo.pact;

import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

public abstract class SopremoReduce<IK extends JsonNode, IV extends JsonNode, OK extends JsonNode, OV extends JsonNode>
		extends ReduceStub<JsonNode, JsonNode, JsonNode, JsonNode> {
	private EvaluationContext context;

	@Override
	public Class<JsonNode> getInKeyType() {
		return SopremoUtil.WRAPPER_TYPE;
	}

	@Override
	public Class<JsonNode> getInValueType() {
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

	protected boolean needsResettableIterator(final JsonNode key, final Iterator<JsonNode> values) {
		return false;
	}

	protected abstract void reduce(JsonNode key, ArrayNode values, JsonCollector out);

	@Override
	public void reduce(final JsonNode key, Iterator<JsonNode> values,
			final Collector<JsonNode, JsonNode> out) {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isTraceEnabled()) {
			final ArrayList<JsonNode> cached = new ArrayList<JsonNode>();
			while (values.hasNext())
				cached.add(values.next());
			SopremoUtil.LOG.trace(String.format("%s %s/%s", this.getContext().operatorTrace(), key, cached));
			values = cached.iterator();
		}
		try {
			this.reduce(SopremoUtil.unwrap(key),
				JsonUtil.wrapWithNode(this.needsResettableIterator(key, values), new WrapperIterator(values)),
				new JsonCollector(out));
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with k/v %s/%s: %s", this.getContext()
				.operatorTrace(),
				key, values, e));
			throw e;
		}
	}
}
