package eu.stratosphere.sopremo;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactJsonObject;

public abstract class SopremoReduce<IK extends Key, IV extends PactJsonObject, OK extends Key, OV extends PactJsonObject>
		extends ReduceStub<IK, IV, OK, OV> {
	private Evaluable transformation;

	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.transformation = SopremoUtil.getObject(parameters, "transformation", Evaluable.class);
		this.context = SopremoUtil.getObject(parameters, "context", EvaluationContext.class);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected Evaluable getTransformation() {
		return this.transformation;
	}
}
