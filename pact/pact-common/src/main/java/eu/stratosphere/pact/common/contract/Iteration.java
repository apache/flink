package eu.stratosphere.pact.common.contract;

import eu.stratosphere.pact.common.plan.Visitor;


/**
 *
 *
 * @author Stephan Ewen
 */
public class Iteration extends Contract
{
	
	
	public Iteration() {
		super("Iteration");
	}
	
	/**
	 * @param name
	 */
	public Iteration(String name) {
		super(name);
	}

	public void setInitialPartialSolution(Contract input)
	{}
	
	public void setNextPartialSolution(Contract result)
	{}
	
	public Contract getPartialSolution()
	{
		return null;
	}
	
	public void setTerminationCriterion(Contract criterion)
	{}
	
	public void setNumberOfIteration(int num)
	{}
	
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
