package eu.stratosphere.pact.common.contract;

import eu.stratosphere.pact.common.plan.Visitor;


/**
 *
 *
 * @author Stephan Ewen
 */
public class Iteration extends Contract
{
	
	
	
	public void setInitialPartialSolution(Contract input)
	{}
	
	public void setIterationResult(Contract result)
	{}
	
	public Contract getIterationInput()
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
		// TODO Auto-generated method stub

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
