package eu.stratosphere.sopremo.pact;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;

public abstract class SopremoReduce<IK extends PactJsonObject.Key, IV extends PactJsonObject, OK extends PactJsonObject.Key, OV extends PactJsonObject>
		extends ReduceStub<IK, IV, OK, OV> {
	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.context = SopremoUtil.deserialize(parameters, "context", EvaluationContext.class);
	}

	protected EvaluationContext getContext() {
		return this.context;
	}
}
