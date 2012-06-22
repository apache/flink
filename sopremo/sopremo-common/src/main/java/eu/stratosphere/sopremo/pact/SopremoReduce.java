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
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class SopremoReduce extends ReduceStub {
	private EvaluationContext context;

	private JsonCollector collector;

	private RecordToJsonIterator cachedIterator;

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
		this.cachedIterator = new RecordToJsonIterator(this.context.getInputSchema(0));
		this.collector = new JsonCollector(this.context.getOutputSchema(0));
		SopremoUtil.configureStub(this, parameters);
	}

	protected final EvaluationContext getContext() {
		return this.context;
	}

	@SuppressWarnings("unused")
	protected boolean needsResettableIterator(final Iterator<IJsonNode> values) {
		return false;
	}

	protected abstract void reduce(IArrayNode values, JsonCollector out);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.ReduceStub#reduce(java.util.Iterator,
	 * eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void reduce(final Iterator<PactRecord> records, final Collector<PactRecord> out) throws Exception {
		this.context.increaseInputCounter();
		this.collector.configure(out, this.context);
		this.cachedIterator.setIterator(records);
		Iterator<IJsonNode> values = this.cachedIterator;
		if (SopremoUtil.LOG.isTraceEnabled()) {
			final ArrayList<IJsonNode> cached = new ArrayList<IJsonNode>();
			while (this.cachedIterator.hasNext())
				cached.add(this.cachedIterator.next());
			values = cached.iterator();
			SopremoUtil.LOG.trace(String.format("%s %s", this.getContext().operatorTrace(), cached));
		}

		final ArrayNode array = JsonUtil.wrapWithNode(this.needsResettableIterator(values), values);
		try {
			this.reduce(array, this.collector);
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with %s: %s", this.getContext().operatorTrace(),
				array, e));
			throw e;
		}
	}
}
