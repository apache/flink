package eu.stratosphere.sopremo.base;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IJsonNode;

@InputCardinality(1)
@OutputCardinality(1)
@Name(verb = "transform")
public class Projection extends ElementaryOperator<Projection> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2170992457478875950L;

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		if (this.getResultProjection() == EvaluationExpression.VALUE)
			return this.createShortCircuitModule();
		return super.asPactModule(context);
	}

	public static class ProjectionStub extends SopremoMap {

		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			out.collect(value);
		}
	}

}
