package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;

public abstract class SopremoCoGroup<IK extends Key, IV1 extends PactJsonObject, IV2 extends PactJsonObject, OK extends Key, OV extends PactJsonObject>
		extends CoGroupStub<IK, IV1, IV2, OK, OV> {
	private Evaluable transformation;

	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.transformation = SopremoUtil.deserialize(parameters, "transformation", Evaluable.class);
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}

	protected Evaluable getTransformation() {
		return this.transformation;
	}
}
