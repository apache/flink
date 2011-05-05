package eu.stratosphere.sopremo.operator;

import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Condition;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.Transformation;

public class Selection extends ConditionalOperator {

	public Selection(Condition condition, Operator input) {
		super(Transformation.IDENTITY, condition, input);
	}

	public static class SelectionStub extends MapStub<PactNull, PactJsonObject, PactNull, PactJsonObject> {
		private Condition condition;

		@Override
		public void configure(Configuration parameters) {
			this.condition = getCondition(parameters, "condition");
		}

		@Override
		public void map(PactNull key, PactJsonObject value, Collector<PactNull, PactJsonObject> out) {
			if (this.condition.evaluate(value.getValue()) == BooleanNode.TRUE)
				out.collect(key, value);
		}

	}

	@Override
	public PactModule asPactModule() {
		PactModule module = new PactModule(1, 1);
		MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject> selectionMap = new MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject>(
			SelectionStub.class);
		module.getOutput(0).setInput(selectionMap);
		selectionMap.setInput(module.getInput(0));
		setCondition(selectionMap.getStubParameters(), "condition", this.getCondition());
		return module;
	}
}
