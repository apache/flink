package eu.stratosphere.client;

import eu.stratosphere.api.Job;
import eu.stratosphere.nephele.client.JobExecutionResult;

public interface PlanExecutor {

	/**
	 * Execute the given plan and return the runtime in milliseconds.
	 * 
	 * @param plan The plan of the program to execute.
	 * @return The net runtime of the program, in milliseconds.
	 * 
	 * @throws Exception Thrown, i job submission caused an exception.
	 */
	public abstract JobExecutionResult executePlan(Job plan) throws Exception;

}