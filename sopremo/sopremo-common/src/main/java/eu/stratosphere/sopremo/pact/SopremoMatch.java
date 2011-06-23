package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;

public abstract class SopremoMatch<IK extends PactJsonObject.Key, IV1 extends PactJsonObject, IV2 extends PactJsonObject, OK extends PactJsonObject.Key, OV extends PactJsonObject>
		extends MatchStub<IK, IV1, IV2, OK, OV> {
	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}
}
