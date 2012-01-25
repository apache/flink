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

	private JsonCollector collector;

	private RecordToJsonIterator cachedIterator1, cachedIterator2;

	protected abstract void coGroup(ArrayNode values1, ArrayNode values2, JsonCollector out);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.CoGroupStub#coGroup(java.util.Iterator, java.util.Iterator,
	 * eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void coGroup(Iterator<PactRecord> records1, Iterator<PactRecord> records2, Collector out) {
		this.context.increaseInputCounter();
		this.cachedIterator1.setIterator(records1);
		this.cachedIterator2.setIterator(records2);
		Iterator<JsonNode> values1 = this.cachedIterator1;
		Iterator<JsonNode> values2 = this.cachedIterator2;
		if (SopremoUtil.LOG.isTraceEnabled()) {
			final ArrayList<JsonNode> cached1 = new ArrayList<JsonNode>(), cached2 = new ArrayList<JsonNode>();
			while (values1.hasNext())
				cached1.add(values1.next());
			while (values2.hasNext())
				cached2.add(values2.next());
			SopremoUtil.LOG.trace(String
				.format("%s %s/%s", this.getContext().operatorTrace(), cached1, cached2));
			values1 = cached1.iterator();
			values2 = cached2.iterator();
		}

		ArrayNode array1 = JsonUtil.wrapWithNode(this.needsResettableIterator(0, values1), values1);
		ArrayNode array2 = JsonUtil.wrapWithNode(this.needsResettableIterator(0, values1), values1);
		try {
			this.coGroup(array1, array2, this.collector);
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with %s/%s: %s", this.getContext()
				.operatorTrace(), array1, array2, e));
			throw e;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		this.context = SopremoUtil.deserialize(parameters, SopremoUtil.CONTEXT, EvaluationContext.class);
		this.collector = new JsonCollector(this.context.getInputSchema(0));
		this.cachedIterator1 = new RecordToJsonIterator(this.context.getInputSchema(0));
		this.cachedIterator2 = new RecordToJsonIterator(this.context.getInputSchema(1));
		SopremoUtil.configureStub(this, parameters);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected boolean needsResettableIterator(final int input, final Iterator<JsonNode> values) {
		return false;
	}
}
