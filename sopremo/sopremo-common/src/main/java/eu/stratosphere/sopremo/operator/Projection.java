package eu.stratosphere.sopremo.operator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.Operator;

public class Projection extends Operator {

	public static class ProjectionStub extends MapStub<PactNull, PactJsonObject, PactNull, PactJsonObject> {
		private Evaluable transformation;

		@Override
		public void configure(Configuration parameters) {
			this.transformation = getEvaluableExpression(parameters, "transformation");
		}

		@Override
		public void map(PactNull key, PactJsonObject value, Collector<PactNull, PactJsonObject> out) {
			out.collect(key,
				new PactJsonObject(this.transformation.evaluate(value.getValue())));
		}
	}

	public Projection(Evaluable transformation, Operator input) {
		super(transformation, input);
	}

	@Override
	public PactModule asPactModule() {
		PactModule module = new PactModule(1, 1);
		MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject> projectionMap = new MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject>(
			ProjectionStub.class);
		module.getOutput(0).setInput(projectionMap);
		projectionMap.setInput(module.getInput(0));
		setEvaluableExpression(projectionMap.getStubParameters(), "transformation", this.getEvaluableExpression());
		return module;
	}

}
