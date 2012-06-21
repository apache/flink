package eu.stratosphere.sopremo.pact;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IJsonNode;

public class JsonCollector {
	private Collector collector;

	private EvaluationContext context;

	private final Schema schema;

	private PactRecord record;

	private CachingExpression<IJsonNode> resultProjection = CachingExpression.ofSubclass(EvaluationExpression.VALUE,
		IJsonNode.class);

	public JsonCollector(final Schema schema) {
		this.schema = schema;
	}

	/**
	 * Sets the collector to the specified value.
	 * 
	 * @param collector
	 *        the collector to set
	 */
	public void configure(final Collector collector, EvaluationContext context) {
		this.collector = collector;
		this.context = context;
		this.resultProjection.setInnerExpression(context.getResultProjection());
	}

	public void collect(final IJsonNode value) {
		if (SopremoUtil.LOG.isTraceEnabled())
			SopremoUtil.LOG.trace(String.format(" to %s", value));
		this.collector.collect(this.record =
			this.schema.jsonToRecord(this.resultProjection.evaluate(value, this.context), this.record, this.context));
	}
}
