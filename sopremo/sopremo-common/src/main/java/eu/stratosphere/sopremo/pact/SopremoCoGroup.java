package eu.stratosphere.sopremo.pact;

import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.OneTimeArrayNode;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * An abstract implementation of the {@link CoGroupStub}. SopremoCoGroup provides the functionality to convert the
 * standard input of the CoGroupStub to a more manageable representation (both inputs are converted to an
 * {@link IArrayNode}).
 */
public abstract class SopremoCoGroup extends CoGroupStub {
	private EvaluationContext context;

	private JsonCollector collector;

	private RecordToJsonIterator cachedIterator1, cachedIterator2;

	private OneTimeArrayNode leftArray = new OneTimeArrayNode(), rightArray = new OneTimeArrayNode();

	private boolean needsResettableIteratorLeft = false, needsResettableIteratorRight = false;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.CoGroupStub#coGroup(java.util.Iterator, java.util.Iterator,
	 * eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void coGroup(final Iterator<PactRecord> records1, final Iterator<PactRecord> records2,
			final Collector<PactRecord> out) {
		this.context.increaseInputCounter();
		this.collector.configure(out, this.context);
		this.cachedIterator1.setIterator(records1);
		this.cachedIterator2.setIterator(records2);

		IArrayNode leftArray = this.needsResettableIteratorLeft ? new ArrayNode(this.leftArray) : this.leftArray;
		IArrayNode rightArray = this.needsResettableIteratorRight ? new ArrayNode(this.rightArray) : this.rightArray;

		if (SopremoUtil.LOG.isTraceEnabled()) {
			if (!this.needsResettableIteratorLeft)
				leftArray = new ArrayNode(leftArray);
			if (!this.needsResettableIteratorRight)
				rightArray = new ArrayNode(rightArray);

			SopremoUtil.LOG.trace(String
				.format("%s %s/%s", this.getContext().operatorTrace(), leftArray, rightArray));
		}

		try {
			this.coGroup(leftArray, rightArray, this.collector);
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with %s/%s: %s", this.getContext()
				.operatorTrace(), leftArray, rightArray, e));
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
		this.leftArray.setNodeIterator(this.cachedIterator1);
		this.rightArray.setNodeIterator(this.cachedIterator2);
		this.needsResettableIteratorLeft = needsResettableIterator(true);
		this.needsResettableIteratorRight = needsResettableIterator(false);
	}

	/**
	 * This method must be implemented to provide a user implementation of a CoGroup.
	 * 
	 * @param values1
	 *        an {@link IArrayNode} that holds all elements of the first input which were paired with the key
	 * @param values2
	 *        an {@link IArrayNode} that holds all elements of the second input which were paired with the key
	 * @param out
	 *        a collector that collects all output pairs
	 */
	protected abstract void coGroup(IArrayNode values1, IArrayNode values2, JsonCollector out);

	protected final EvaluationContext getContext() {
		return this.context;
	}

	protected boolean needsResettableIterator(final boolean left) {
		final MultipassValues annotation = ReflectUtil.getAnnotation(getClass(), MultipassValues.class);
		return left ? annotation.left() : annotation.right();
	}
}
