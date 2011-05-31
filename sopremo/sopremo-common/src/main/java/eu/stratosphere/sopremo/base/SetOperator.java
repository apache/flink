package eu.stratosphere.sopremo.base;

import java.util.List;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public abstract class SetOperator extends Operator {

	private EvaluableExpression[] setKeyExtractors;

	public SetOperator(Operator... inputs) {
		super(EvaluableExpression.IDENTITY, inputs);
		this.setKeyExtractors = new EvaluableExpression[inputs.length];
		for (int index = 0; index < this.setKeyExtractors.length; index++) 
			this.setKeyExtractors[index] = new Input(index);
	}

	public SetOperator(List<Operator> inputs) {
		super(EvaluableExpression.IDENTITY, inputs);
		this.setKeyExtractors = new EvaluableExpression[inputs.size()];
		for (int index = 0; index < this.setKeyExtractors.length; index++) 
			this.setKeyExtractors[index] = new Input(index);
	}

	public void setKeyExtractors(Path... keyExtractors) {
		if (setKeyExtractors == null)
			throw new NullPointerException("setKeyExtractors must not be null");

		// ensures size
		for (Path keyExtractor : keyExtractors) {
			int inputIndex = SopremoUtil.getInputIndex(keyExtractor);
			if (inputIndex == -1)
				throw new IllegalArgumentException("extractor does not contain input selector: " + keyExtractor);
			this.setKeyExtractors[inputIndex] = keyExtractor;
		}
	}

	public EvaluableExpression getSetKeyExtractor(JsonStream input) {
		int index = getInputs().indexOf(input.getSource());
		if (index == -1)
			throw new IllegalArgumentException();
		return this.setKeyExtractors[index];
	}

	public EvaluableExpression getSetKeyExtractor(int index) {
		return this.setKeyExtractors[index];
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		int numInputs = this.getInputOperators().size();
		PactModule module = new PactModule(numInputs, 1);
		
		if(numInputs == 1) {
			module.getOutput(0).setInput(module.getInput(0));
			return module;
		}

		Contract leftInput = SopremoUtil.addKeyExtraction(module, getSetKeyExtractor(0), context);
		for (int index = 1; index < numInputs; index++) {

			Contract rightInput = SopremoUtil.addKeyExtraction(module, getSetKeyExtractor(index), context);
			Contract union = createSetContractForInputs(leftInput, rightInput);

			SopremoUtil.setTransformationAndContext(union.getStubParameters(), null, context);
			leftInput = union;
		}
		
		module.getOutput(0).setInput(SopremoUtil.addKeyRemover(leftInput));

		return module;
	}

	protected abstract Contract createSetContractForInputs(Contract leftInput, Contract rightInput);
}
