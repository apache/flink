package eu.stratosphere.sopremo.operator;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.Transformation;

public class Projection extends Operator {

	public static class ProjectionStub extends MapStub<PactNull, PactJsonObject, PactNull, PactJsonObject> {
		private Transformation transformation;

		@Override
		public void configure(Configuration parameters) {
			this.transformation = getTransformation(parameters, "transformation");
		}

		@Override
		public void map(PactNull key, PactJsonObject value, Collector<PactNull, PactJsonObject> out) {
			JsonNode transformedObject = this.transformation.evaluate(value.getValue());
			out.collect(key, new PactJsonObject(transformedObject));
		}

	}

	public Projection(Transformation transformation, Operator input) {
		super(transformation, input);
	}

	@Override
	public PactModule asPactModule() {
		PactModule module = new PactModule(1, 1);
		MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject> projectionMap = new MapContract<PactNull, PactJsonObject, PactNull, PactJsonObject>(
			ProjectionStub.class);
		module.getOutput(0).setInput(projectionMap);
		projectionMap.setInput(module.getInput(0));
		setTransformation(projectionMap.getStubParameters(), "transformation", this.getTransformation());
		return module;
	}

}
