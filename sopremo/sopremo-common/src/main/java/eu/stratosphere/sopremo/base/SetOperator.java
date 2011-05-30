package eu.stratosphere.sopremo.base;

import java.util.List;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoUtil;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Path;

public abstract class SetOperator extends Operator {

	private Path[] setKeyExtractors;

	public SetOperator(Operator... inputs) {
		super(EvaluableExpression.IDENTITY, inputs);
		this.setKeyExtractors = new Path[inputs.length];
	}

	public SetOperator(List<Operator> inputs) {
		super(EvaluableExpression.IDENTITY, inputs);
		this.setKeyExtractors = new Path[inputs.size()];
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

	public Path getSetKeyExtractor(JsonStream input) {
		int index = getInputs().indexOf(input.getSource());
		if (index == -1)
			throw new IllegalArgumentException();
		return this.setKeyExtractors[index];
	}

	public Path getSetKeyExtractor(int index) {
		return this.setKeyExtractors[index];
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		int numInputs = this.getInputOperators().size();
		PactModule module = new PactModule(numInputs, 1);

		Contract leftInput = SopremoUtil.addKeyExtraction(module, getSetKeyExtractor(0), context);
		for (int index = 1; index < numInputs; index++) {

			Contract rightInput = SopremoUtil.addKeyExtraction(module, getSetKeyExtractor(index), context);
			Contract union = createSetContractForInputs(leftInput, rightInput);

			SopremoUtil.setTransformationAndContext(union.getStubParameters(), null, context);
			leftInput = union;
		}

		module.getOutput(0).setInput(leftInput);

		return module;
	}

	protected abstract Contract createSetContractForInputs(Contract leftInput, Contract rightInput);
}
