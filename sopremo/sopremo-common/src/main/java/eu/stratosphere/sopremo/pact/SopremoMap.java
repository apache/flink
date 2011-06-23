package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;

public abstract class SopremoMap<IK extends PactJsonObject.Key, IV extends PactJsonObject, OK extends PactJsonObject.Key, OV extends PactJsonObject>
		extends MapStub<IK, IV, OK, OV> {
	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}
}
