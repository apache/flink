package eu.stratosphere.sopremo.operator;

import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.Condition;
import eu.stratosphere.sopremo.expressions.Transformation;

public class Selection extends ConditionalOperator {

	public Selection(Condition condition, Operator input) {
		super(Transformation.IDENTITY, condition, input);
	}

	public static class SelectionStub extends SopremoMap<PactNull, PactJsonObject, Key, PactJsonObject> {
		private Condition condition;

		@Override
		public void configure(Configuration parameters) {
			this.condition = PactUtil.getObject(parameters, "condition", Condition.class);
		}

		@Override
		public void map(PactNull key, PactJsonObject value, Collector<Key, PactJsonObject> out) {
			if (this.condition.evaluate(value.getValue(), getContext()) == BooleanNode.TRUE)
				out.collect(key, value);
		}

	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		PactModule module = new PactModule(1, 1);
		MapContract<PactNull, PactJsonObject, Key, PactJsonObject> selectionMap = new MapContract<PactNull, PactJsonObject, Key, PactJsonObject>(
			SelectionStub.class);
		module.getOutput(0).setInput(selectionMap);
		selectionMap.setInput(module.getInput(0));
		PactUtil.setObject(selectionMap.getStubParameters(), "condition", this.getCondition());
		PactUtil.setTransformationAndContext(selectionMap.getStubParameters(), null, context);
		return module;
	}
}
