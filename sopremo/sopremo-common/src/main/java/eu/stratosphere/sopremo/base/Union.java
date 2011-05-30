package eu.stratosphere.sopremo.base;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.DataStream;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoUtil;
import eu.stratosphere.sopremo.SopremoCoGroup;
import eu.stratosphere.sopremo.base.Selection.SelectionStub;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.Input;
import eu.stratosphere.sopremo.expressions.Path;

public class Union extends Operator {
	public final static Path[] BAG_SEMANTIC = new Path[0];

	private List<Path> setKeyExtractors = new ArrayList<Path>();

	public void setWithSetSemantics(Path... setKeyExtractors) {
		if (setKeyExtractors == null)
			throw new NullPointerException("setKeyExtractors must not be null");

		if (setKeyExtractors == BAG_SEMANTIC)
			this.setKeyExtractors.clear();
		else {
			// ensures size
			this.setKeyExtractors.addAll(Arrays.asList(new Path[Math.max(0,
				getInputs().size() - this.setKeyExtractors.size())]));
			for (Path path : setKeyExtractors)
				this.setKeyExtractors.set(SopremoUtil.getInputIndex(path), path);
			for (int index = 0; index < this.setKeyExtractors.size(); index++)
				if (this.setKeyExtractors.get(index) == null)
					this.setKeyExtractors.set(index, new Path(new Input(index)));
		}
	}

	public Union withSetSemantics(Path... setKeyExtractors) {
		setWithSetSemantics(setKeyExtractors);
		return this;
	}

	public boolean isWithSetSemantic() {
		return setKeyExtractors.size() > 0;
	}

	public Path getSetKeyExtractor(DataStream input) {
		if (!isWithSetSemantic())
			return null;
		int index = getInputs().indexOf(input.getSource());
		if (index == -1)
			throw new IllegalArgumentException();
		return this.setKeyExtractors.get(index);
	}

	public Path getSetKeyExtractor(int index) {
		if (!isWithSetSemantic())
			return null;
		return this.setKeyExtractors.get(index);
	}

	public Union(Operator... inputs) {
		super(EvaluableExpression.IDENTITY, inputs);
	}

	public Union(List<Operator> inputs) {
		super(EvaluableExpression.IDENTITY, inputs);
	}

	// TODO: replace with efficient union operator
	public static class BagUnion extends
			SopremoCoGroup<PactNull, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		@Override
		public void coGroup(PactNull key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactNull, PactJsonObject> out) {
			while (values1.hasNext())
				out.collect(key, values1.next());
			while (values2.hasNext())
				out.collect(key, values2.next());
		}
	}

	public static class SetUnion extends
			SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> {
		@Override
		public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1, Iterator<PactJsonObject> values2,
				Collector<PactNull, PactJsonObject> out) {
			System.out.println(key);
			if (values1.hasNext())
				out.collect(PactNull.getInstance(), values1.next());
			else if (values2.hasNext())
				out.collect(PactNull.getInstance(), values2.next());
		}
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		int numInputs = this.getInputOperators().size();
		PactModule module = new PactModule(numInputs, 1);

		if (isWithSetSemantic()) {
			Contract leftInput = SopremoUtil.addKeyExtraction(module, getSetKeyExtractor(0), context);
			for (int index = 1; index < numInputs; index++) {

				Contract rightInput = SopremoUtil.addKeyExtraction(module, getSetKeyExtractor(index), context);
				CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> union = new CoGroupContract<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactNull, PactJsonObject>(
					SetUnion.class);
				union.setFirstInput(leftInput);
				union.setSecondInput(rightInput);

				SopremoUtil.setTransformationAndContext(union.getStubParameters(), null, context);
				leftInput = union;
			}

			module.getOutput(0).setInput(leftInput);
		} else {
			Contract leftInput = module.getInput(0);
			for (int index = 1; index < numInputs; index++) {

				Contract rightInput = module.getInput(index);
				CoGroupContract<PactNull, PactJsonObject, PactJsonObject, PactNull, PactJsonObject> union = new CoGroupContract<PactNull, PactJsonObject, PactJsonObject, PactNull, PactJsonObject>(
					BagUnion.class);
				union.setFirstInput(leftInput);
				union.setSecondInput(rightInput);

				SopremoUtil.setTransformationAndContext(union.getStubParameters(), null, context);
				leftInput = union;
			}

			module.getOutput(0).setInput(leftInput);
		}

		return module;
	}
}
