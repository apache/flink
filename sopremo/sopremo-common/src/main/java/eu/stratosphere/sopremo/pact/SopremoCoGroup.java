package eu.stratosphere.sopremo.pact;

import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

public abstract class SopremoCoGroup extends CoGroupStub {
	private EvaluationContext context;

	protected abstract void coGroup(ArrayNode values1, ArrayNode values2, JsonCollector out);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.CoGroupStub#coGroup(java.util.Iterator, java.util.Iterator,
	 * eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void coGroup(Iterator<PactRecord> records1, Iterator<PactRecord> records2, Collector out) {
		this.context.increaseInputCounter();
		if (SopremoUtil.LOG.isTraceEnabled()) {
			final ArrayList<JsonNode> cached1 = new ArrayList<JsonNode>(), cached2 = new ArrayList<JsonNode>();
			while (records1.hasNext())
				cached1.add(records1.next());
			while (records2.hasNext())
				cached2.add(records2.next());
			SopremoUtil.LOG.trace(String
				.format("%s %s/%s/%s", this.getContext().operatorTrace(), key, cached1, cached2));
			values1 = cached1.iterator();
			values2 = cached2.iterator();
		}
		try {
			this.coGroup(SopremoUtil.unwrap(key),
				JsonUtil.wrapWithNode(this.needsResettableIterator(0, key, values1), new WrapperIterator(values1)),
				JsonUtil.wrapWithNode(this.needsResettableIterator(0, key, values2), new WrapperIterator(values2)),
				new JsonCollector(out));
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with k/v/v %s/%s/%s: %s", this.getContext()
				.operatorTrace(), key, values1, values2, e));
			throw e;
		}
	}

	@Override
	public void configure(final Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
		SopremoUtil.configureStub(this, parameters);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected boolean needsResettableIterator(final int input, final JsonNode key,
			final Iterator<JsonNode> values) {
		return false;
	}
}
