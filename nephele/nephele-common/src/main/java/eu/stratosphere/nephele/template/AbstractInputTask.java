/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.template;

/**
 * Abstract base class for tasks submitted as a part of a job input vertex.
 * 
 * @author warneke
 */
public abstract class AbstractInputTask extends AbstractInvokable
{
	/**
	 * This method computes the different splits of the input that can be processed in parallel. It needs
	 * to be implemented by classes that describe input tasks. 
	 * <p>
	 * Note that this method does not return the input splits for the task instance only, but it computes all 
	 * splits for all parallel instances. Those computed splits are then assigned to the individual task instances 
	 * by the Job Manager. To obtain the input splits for the current task instance, use the 
	 * {@link #getTaskInputSplits()} method. 
	 * 
	 * @param requestedMinNumber The minimum number of splits to create. This is a hint by the system how many splits
	 *                           should be generated at least (typically because there are that many parallel task
	 *                           instances), but it is no hard constraint. 
	 * 
	 * @return The input splits for the input to be processed by all instances of this input task. 
	 */
	public abstract InputSplit[] computeInputSplits(int requestedMinNumber) throws Exception;
	
	/**
	 * Gets the input splits that are to be worked on by this specific instance of the input task.
	 * 
	 * @return The input splits that are to be worked on by this specific instance of the input task.
	 */
	public InputSplit[] getTaskInputSplits()
	{
		return getEnvironment().getInputSplits();
	}
}