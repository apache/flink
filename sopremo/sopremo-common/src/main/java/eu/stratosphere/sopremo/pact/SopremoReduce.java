package eu.stratosphere.sopremo.pact;

import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.JsonNode;

public abstract class SopremoReduce extends ReduceStub {
	private EvaluationContext context;

	private JsonCollector collector;

	private RecordToJsonIterator cachedIterator;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		this.context = SopremoUtil.deserialize(parameters, SopremoUtil.CONTEXT, EvaluationContext.class);
		this.cachedIterator = new RecordToJsonIterator(this.context.getInputSchema(0));
		this.collector = new JsonCollector(this.context.getOutputSchema(0));
		SopremoUtil.configureStub(this, parameters);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected boolean needsResettableIterator(final Iterator<JsonNode> values) {
		return false;
	}

	protected abstract void reduce(ArrayNode values, JsonCollector out);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.ReduceStub#reduce(java.util.Iterator,
	 * eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void reduce(Iterator<PactRecord> records, Collector out) throws Exception {
		this.context.increaseInputCounter();
		this.collector.setCollector(out);
		this.cachedIterator.setIterator(records);
		Iterator<JsonNode> values = this.cachedIterator;
		if (SopremoUtil.LOG.isTraceEnabled()) {
			final ArrayList<JsonNode> cached = new ArrayList<JsonNode>();
			while (this.cachedIterator.hasNext())
				cached.add(this.cachedIterator.next());
			values = cached.iterator();
			SopremoUtil.LOG.trace(String.format("%s %s", this.getContext().operatorTrace(), values));
		}

		ArrayNode array = JsonUtil.wrapWithNode(this.needsResettableIterator(values), values);
		try {
			this.reduce(array, this.collector);
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with %s: %s", this.getContext().operatorTrace(),
				array, e));
			throw e;
		}
	}
}
