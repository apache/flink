package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.sopremo.type.Schema;

public abstract class SopremoCross extends CrossStub {
	private EvaluationContext context;

	private Schema inputSchema1, inputSchema2;

	private JsonCollector collector;

	private JsonNode cachedInput1, cachedInput2;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.Stub#open(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		this.context = SopremoUtil.deserialize(parameters, SopremoUtil.CONTEXT, EvaluationContext.class);
		this.inputSchema1 = this.context.getInputSchema(0);
		this.inputSchema2 = this.context.getInputSchema(1);
		this.collector = new JsonCollector(this.context.getOutputSchema(0));
		SopremoUtil.configureStub(this, parameters);
	}

	protected abstract void cross(JsonNode value1, JsonNode value2, JsonCollector out);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.stubs.CrossStub#cross(eu.stratosphere.pact.common.type.PactRecord,
	 * eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
	 */
	@Override
	public void cross(PactRecord record1, PactRecord record2, Collector out) {
		this.context.increaseInputCounter();
		this.collector.setCollector(out);
		JsonNode input1 = this.inputSchema1.recordToJson(record1, this.cachedInput1);
		JsonNode input2 = this.inputSchema2.recordToJson(record2, this.cachedInput2);
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
