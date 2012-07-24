package eu.stratosphere.sopremo.base;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.ElementarySopremoModule;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Unifies the input json streams in a bag semantic.
 * 
 * @author Arvid Heise
 */
// efficient implementation using pact union all mechanism
// @Name(verb = "union all")
// @InputCardinality(min = 1, max = Integer.MAX_VALUE)
// public class UnionAll extends ElementaryOperator<UnionAll> {
// /**
// *
// */
// private static final long serialVersionUID = 8160253166221264064L;
//
// /*
// * (non-Javadoc)
// * @see eu.stratosphere.sopremo.ElementaryOperator#asPactModule(eu.stratosphere.sopremo.EvaluationContext)
// */
// @Override
// public PactModule asPactModule(EvaluationContext context) {
// final List<JsonStream> inputs = this.getInputs();
// final PactModule module = new PactModule(this.getName(), inputs.size(), 1);
// // TODO: remove identity map, when Nephele can deal with direct source->sink connections
// MapContract identityContract = new MapContract(IdentityMap.class);
// for (Contract input : module.getInputs())
// identityContract.addInput(input);
// module.getOutput(0).setInput(identityContract);
// return module;
// }
// }
// slow implementation using cogroups
@Name(verb = "union all")
@InputCardinality(min = 1, max = Integer.MAX_VALUE)
@OutputCardinality(1)
public class UnionAll extends CompositeOperator<UnionAll> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8160253166221264064L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.CompositeOperator#asElementaryOperators(eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public ElementarySopremoModule asElementaryOperators(EvaluationContext context) {
		final int numInputs = getInputs().size();
		final ElementarySopremoModule module = new ElementarySopremoModule(getName(), numInputs, 1);
		JsonStream lastResult = module.getInput(0);
		for (int index = 1; index < numInputs; index++) 
			lastResult = new TwoSourceUnionAll().withInputs(lastResult, module.getInput(index));
		module.getOutput(0).setInput(0, lastResult);
		return module;
	}

	@InputCardinality(2)
	public static class TwoSourceUnionAll extends ElementaryOperator<TwoSourceUnionAll> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		/**
		 * Initializes UnionAll.TwoSourceUnionAll.
		 *
		 */
		public TwoSourceUnionAll() {
			setKeyExpressions(0, ALL_KEYS);
			setKeyExpressions(1, ALL_KEYS);
		}

		public static class Implementation extends SopremoCoGroup {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IArrayNode,
			 * eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void coGroup(IArrayNode values1, IArrayNode values2, JsonCollector out) {
				for (IJsonNode node : values1)
					out.collect(node);
				for (IJsonNode node : values2)
					out.collect(node);
			}
		}
	}
}