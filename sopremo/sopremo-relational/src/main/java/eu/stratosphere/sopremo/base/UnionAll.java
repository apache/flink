package eu.stratosphere.sopremo.base;

import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.base.Union.TwoInputUnion;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;

public class UnionAll extends CompositeOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8160253166221264064L;

	public UnionAll(final List<Operator> inputs) {
		super(inputs);
	}

	public UnionAll(final Operator... inputs) {
		super(inputs);
	}

	@Override
	public SopremoModule asElementaryOperators() {
		
		final List<Output> inputs = getInputs();
		final SopremoModule module = new SopremoModule(getName(), inputs.size(), 1);
		
		Operator leftInput = module.getInput(0);
		for (int index = 1; index < inputs.size(); index++)
			leftInput = new TwoInputUnion(leftInput, module.getInput(index));
		
		module.getOutput(0).setInput(0, leftInput);
		
		return module;
	}

	// @Override
	// public PactModule asPactModule(final EvaluationContext context) {
	// final int numInputs = this.getInputOperators().size();
	// final PactModule module = new PactModule(this.toString(), numInputs, 1);
	//
	// Contract leftInput = module.getInput(0);
	// for (int index = 1; index < numInputs; index++) {
	//
	// // final Contract rightInput = module.getInput(index);
	// // final CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject>
	// union =
	// // new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject>(
	// // TwoInputUnion.class);
	// // union.setFirstInput(leftInput);
	// // union.setSecondInput(rightInput);
	// //
	// // SopremoUtil.setContext(union.getParameters(), context);
	// // leftInput = union;
	// }
	//
	// module.getOutput(0).setInput(leftInput);
	//
	// return module;
	// }

	public static class TwoInputUnionAll extends ElementaryOperator {
		public TwoInputUnionAll(JsonStream input1, JsonStream input2) {
			super(input1, input2);
		}

		// TODO: replace with efficient union operator
		public static class TwoInputUnion extends
				SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void coGroup(final JsonNode key, final StreamArrayNode values1, final StreamArrayNode values2,
					final JsonCollector out) {
				for (final JsonNode value : values1)
					out.collect(key, value);
				for (final JsonNode value : values2)
					out.collect(key, value);
			}
		}
	}
}
