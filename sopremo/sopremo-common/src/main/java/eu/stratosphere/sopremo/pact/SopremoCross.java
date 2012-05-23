package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * An abstract implementation of the {@link CrossStub}. SopremoCross provides the functionality to convert the
 * standard input of the CrossStub to a more manageable representation (both inputs are converted to an
 * {@link IJsonNode}).
 */
public abstract class SopremoCross extends CrossStub {
	private EvaluationContext context;

	private Schema inputSchema1, inputSchema2;

	private JsonCollector collector;

	private IJsonNode cachedInput1, cachedInput2;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void open(final Configuration parameters) throws Exception {
		this.context = SopremoUtil.deserialize(parameters, SopremoUtil.CONTEXT, EvaluationContext.class);
		this.inputSchema1 = this.context.getInputSchema(0);
		this.inputSchema2 = this.context.getInputSchema(1);
		this.collector = new JsonCollector(this.context.getOutputSchema(0));
		SopremoUtil.configureStub(this, parameters);
	}

	/**
	 * This method must be implemented to provide a user implementation of a cross.
	 * 
	 * @param values1
	 *        an {@link IJsonNode} from the first input
	 * @param values2
	 *        an {@link IJsonNode} from the second input
	 * @param out
	 *        a collector that collects all output pairs
	 */
	protected abstract void cross(IJsonNode value1, IJsonNode value2, JsonCollector out);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.CrossStub#cross(eu.stratosphere.pact.common.type.PactRecord,
	 * eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void cross(final PactRecord record1, final PactRecord record2, final Collector out) {
		this.context.increaseInputCounter();
		this.collector.configure(out, this.context);
		final IJsonNode input1 = this.inputSchema1.recordToJson(record1, this.cachedInput1);
		final IJsonNode input2 = this.inputSchema2.recordToJson(record2, this.cachedInput2);
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format("%s %s/%s", this.getContext().operatorTrace(), input1, input2));
		try {
			this.cross(input1, input2, this.collector);
		} catch (final RuntimeException e) {
			SopremoUtil.LOG.error(String.format("Error occurred @ %s with v1 %s/%s v2: %s", this.getContext()
				.operatorTrace(), input1, input2, e));
			throw e;
		}
	}

	protected EvaluationContext getContext() {
		return this.context;
	}
}
