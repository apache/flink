package eu.stratosphere.sopremo;

import java.util.List;

import eu.stratosphere.pact.common.plan.PactModule;

public abstract class CompositeOperator extends Operator {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9172753270465124102L;

	public CompositeOperator(int numberOfOutputs, JsonStream... inputs) {
		super(numberOfOutputs, inputs);
	}

	public CompositeOperator(int numberOfOutputs, List<? extends JsonStream> inputs) {
		super(numberOfOutputs, inputs);
	}

	public CompositeOperator(JsonStream... inputs) {
		super(inputs);
	}

	public CompositeOperator(List<? extends JsonStream> inputs) {
		super(inputs);
	}

	public abstract SopremoModule asElementaryOperators();

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		System.out.println("-------");
		System.out.println(this);
		System.out.println("->");
		SopremoModule elementaryPlan = this.asElementaryOperators();
		System.out.println(elementaryPlan);
		System.out.println("-------");
		return elementaryPlan.asPactModule(context);
	}
}
