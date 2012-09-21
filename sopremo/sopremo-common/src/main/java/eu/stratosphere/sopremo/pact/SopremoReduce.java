package eu.stratosphere.sopremo.pact;

import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.OneTimeArrayNode;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * An abstract implementation of the {@link ReduceStub}. SopremoReduce provides the functionality to convert the
 * standard input of the ReduceStub to a more manageable representation (the input is converted to an {@link IArrayNode}
 * ).
 */
public abstract class SopremoReduce extends ReduceStub {
	private EvaluationContext context;

	private JsonCollector collector;

	private RecordToJsonIterator cachedIterator;

	private final OneTimeArrayNode array = new OneTimeArrayNode(this.cachedIterator);

	private boolean needsResettableIterator = false;

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
		this.array.setNodeIterator(this.cachedIterator);
		this.needsResettableIterator = this.needsResettableIterator();
	}

	protected final EvaluationContext getContext() {
		return this.context;
	}

	protected boolean needsResettableIterator() {
		return ReflectUtil.getAnnotation(getClass(), MultipassValues.class) != null;
	}

	/**
	 * This method must be implemented to provide a user implementation of a reduce.
	 * 
	 * @param values
	 *        an {@link IArrayNode} that holds all elements that belong to the same key
	 * @param out
	 *        a collector that collects all output nodes
	 */
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
		IArrayNode array = this.needsResettableIterator ? new ArrayNode(this.array) : this.array;
		if (SopremoUtil.DEBUG && SopremoUtil.LOG.isTraceEnabled()) {
			if(!this.needsResettableIterator)
				array = new ArrayNode(array);
			SopremoUtil.LOG.trace(String.format("%s %s", this.getContext().operatorTrace(), array));
		}

		try {
			this.reduce(array, this.collector);
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with %s: %s", this.getContext().operatorTrace(),
				array, e));
			throw e;
		}
	}
}
