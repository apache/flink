package eu.stratosphere.sopremo.operator;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;

public abstract class SopremoCross<IK1 extends Key, IV1 extends PactJsonObject, IK2 extends Key, IV2 extends PactJsonObject, OK extends Key, OV extends PactJsonObject>
		extends CrossStub<IK1, IV1, IK2, IV2, OK, OV> {
	private Evaluable transformation;

	private EvaluationContext context;

	@Override
	public void configure(Configuration parameters) {
		this.transformation = PactUtil.getObject(parameters, "transformation", Evaluable.class);
		this.context = PactUtil.getObject(parameters, "context", EvaluationContext.class);
	}

	protected EvaluationContext getContext() {
		return context;
	}

	protected Evaluable getTransformation() {
		return this.transformation;
	}
}
