package eu.stratosphere.sopremo.pact;

import java.util.Iterator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;
import eu.stratosphere.sopremo.type.StreamArrayNode;

/**
 * An abstract implementation of the {@link CoGroupStub}. SopremoCoGroup provides the functionality to convert the
 * standard input of the CoGroupStub to a more manageable representation (both inputs are converted to an
 * {@link IArrayNode}).
 */
public abstract class SopremoCoGroup extends CoGroupStub {
	private EvaluationContext context;

	private JsonCollector collector;

	private RecordToJsonIterator cachedIterator1, cachedIterator2;

	private final StreamArrayNode leftArray = new StreamArrayNode(), rightArray = new StreamArrayNode();

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

		try {
			if (SopremoUtil.DEBUG && SopremoUtil.LOG.isTraceEnabled()) {
				ArrayNode leftArray = new ArrayNode(this.leftArray);
				ArrayNode rightArray = new ArrayNode(this.rightArray);

				SopremoUtil.LOG.trace(String.format("%s %s/%s", this.getContext().operatorTrace(), leftArray,
					rightArray));
				this.coGroup(leftArray, rightArray, this.collector);
			} else {
				this.coGroup(this.leftArray, this.rightArray, this.collector);

			}
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with %s/%s: %s", this.getContext()
				.operatorTrace(), this.leftArray, this.rightArray, e));
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
	}

	/**
	 * This method must be implemented to provide a user implementation of a CoGroup.
	 * 
	 * @param values1
	 *        an {@link OneTimeArrayNode} that holds all elements of the first input which were paired with the key
	 * @param values2
	 *        an {@link OneTimeArrayNode} that holds all elements of the second input which were paired with the key
	 * @param out
	 *        a collector that collects all output pairs
	 */
	protected abstract void coGroup(IStreamArrayNode values1, IStreamArrayNode values2, JsonCollector out);

	protected final EvaluationContext getContext() {
		return this.context;
	}
}
