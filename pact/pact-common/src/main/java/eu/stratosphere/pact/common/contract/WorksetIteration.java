package eu.stratosphere.pact.common.contract;

import eu.stratosphere.pact.common.plan.Visitor;


/**
 *
 *
 * @author Stephan Ewen
 */
public class WorksetIteration extends Contract
{
	
	
	public WorksetIteration() {
		super("blah");
	}
	
	/**
	 * @param name
	 */
	public WorksetIteration(String name) {
		super(name);
	}


	public void setInitialWorkset(Contract input)
	{}
	
	public void setInitialPartialSolution(Contract input)
	{}
	
	public void setNextWorkset(Contract result)
	{}
	
	public void setPartialSolutionDelta(Contract result)
	{}
	
	
	public Contract getPartialSolution()
	{
		return null;
	}
	
	public Contract getWorkset()
	{
		return null;
	}
	
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<Contract> visitor) {
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.contract.Contract#getUserCodeClass()
	 */
	@Override
	public Class<?> getUserCodeClass()
	{
		return null;
	}
}
