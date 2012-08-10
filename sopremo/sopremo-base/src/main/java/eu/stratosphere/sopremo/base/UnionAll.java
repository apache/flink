package eu.stratosphere.sopremo.base;

import java.util.List;

import eu.stratosphere.pact.common.IdentityMap;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Name;

/**
 * Unifies the input json streams in a bag semantic.
 * 
 * @author Arvid Heise
 */
@Name(verb = "union all")
@InputCardinality(min = 1, max = Integer.MAX_VALUE)
public class UnionAll extends ElementaryOperator<UnionAll> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8160253166221264064L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ElementaryOperator#asPactModule(eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public PactModule asPactModule(EvaluationContext context) {		
		final List<JsonStream> inputs = this.getInputs();
		final PactModule module = new PactModule(this.getName(), inputs.size(), 1);
		// TODO: remove identity map, when Nephele can deal with direct source->sink connections
		MapContract identityContract = MapContract.builder(IdentityMap.class).build();
		for (Contract input : module.getInputs())
			identityContract.addInput(input);
		module.getOutput(0).setInput(identityContract);
		return module;
	}
}
