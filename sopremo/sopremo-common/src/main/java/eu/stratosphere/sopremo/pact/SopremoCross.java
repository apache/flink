package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;

public abstract class SopremoCross<IK1 extends PactJsonObject.Key, IV1 extends PactJsonObject, IK2 extends PactJsonObject.Key, IV2 extends PactJsonObject, OK extends Key, OV extends PactJsonObject>
		extends CrossStub<IK1, IV1, IK2, IV2, OK, OV> {
	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}
}
