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
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class SopremoCoGroup extends CoGroupStub {
	private EvaluationContext context;

	private JsonCollector collector;

	private RecordToJsonIterator cachedIterator1, cachedIterator2;

	protected abstract void coGroup(IArrayNode values1, IArrayNode values2, JsonCollector out);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.CoGroupStub#coGroup(java.util.Iterator, java.util.Iterator,
	 * eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void coGroup(final Iterator<PactRecord> records1, final Iterator<PactRecord> records2, final Collector out) {
		this.context.increaseInputCounter();
		this.collector.configure(out, this.context);
		this.cachedIterator1.setIterator(records1);
		this.cachedIterator2.setIterator(records2);
		Iterator<IJsonNode> values1 = this.cachedIterator1;
		Iterator<IJsonNode> values2 = this.cachedIterator2;
		if (SopremoUtil.LOG.isTraceEnabled()) {
			final ArrayList<IJsonNode> cached1 = new ArrayList<IJsonNode>(), cached2 = new ArrayList<IJsonNode>();
			while (values1.hasNext())
				cached1.add(values1.next());
			while (values2.hasNext())
				cached2.add(values2.next());
			SopremoUtil.LOG.trace(String
				.format("%s %s/%s", this.getContext().operatorTrace(), cached1, cached2));
			values1 = cached1.iterator();
			values2 = cached2.iterator();
		}

		final ArrayNode array1 = JsonUtil.wrapWithNode(this.needsResettableIterator(0, values1), values1);
		final ArrayNode array2 = JsonUtil.wrapWithNode(this.needsResettableIterator(0, values2), values2);
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
	public void open(final Configuration parameters) throws Exception {
		// We need to pass our class loader since the default class loader is
		// not able to resolve classes coming from the Sopremo user jar file.
		this.context = SopremoUtil.deserialize(parameters, SopremoUtil.CONTEXT,
				EvaluationContext.class, this.getClass().getClassLoader());
		this.collector = new JsonCollector(this.context.getInputSchema(0));
		this.cachedIterator1 = new RecordToJsonIterator(this.context.getInputSchema(0));
		this.cachedIterator2 = new RecordToJsonIterator(this.context.getInputSchema(1));
		SopremoUtil.configureStub(this, parameters);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}
	
	@SuppressWarnings("unused") 
	protected boolean needsResettableIterator(final int input, final Iterator<IJsonNode> values) {
		return false;
	}
}
