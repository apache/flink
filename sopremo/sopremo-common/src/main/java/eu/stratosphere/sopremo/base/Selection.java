package eu.stratosphere.sopremo.base;

import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.expressions.ConditionalExpression;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class Selection extends ElementaryOperator {
	private final ConditionalExpression condition;

	public Selection(ConditionalExpression condition, JsonStream input) {
		super(input);
		this.condition = condition;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return super.equals(obj) && this.condition.equals(((Selection) obj).condition);
	}

	public ConditionalExpression getCondition() {
		return this.condition;
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result + this.condition.hashCode();
		return result;
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		PactModule module = new PactModule(toString(), 1, 1);
		MapContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> selectionMap =
			new MapContract<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject>(
				SelectionStub.class);
		module.getOutput(0).setInput(selectionMap);
		selectionMap.setInput(module.getInput(0));
		SopremoUtil.serialize(selectionMap.getStubParameters(), "condition", this.getCondition());
		SopremoUtil.setContext(selectionMap.getStubParameters(), context);
		return module;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" on ").append(this.condition);
		return builder.toString();
	}

	public static class SelectionStub extends
			SopremoMap<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
		private ConditionalExpression condition;

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			this.condition = SopremoUtil.deserialize(parameters, "condition", ConditionalExpression.class);
		}

		@Override
		public void map(PactJsonObject.Key key, PactJsonObject value, Collector<PactJsonObject.Key, PactJsonObject> out) {
			if (this.condition.evaluate(value.getValue(), this.getContext()) == BooleanNode.TRUE)
				out.collect(key, value);
		}

	}
}
