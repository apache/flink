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

package eu.stratosphere.nephele.plugins;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.ProfilingListener;

/**
 * This interface must be implemented by every plugin component which is supposed to run inside Nephele's job manager.
 * 
 * @author warneke
 */
public interface JobManagerPlugin {

	/**
	 * Checks whether the plugin requires a job to be executed with profiling enabled in order to work properly.
	 * 
	 * @return <code>true</code> if the job requires profiling to be enabled for a job, <code>false</code> otherwise
	 */
	boolean requiresProfiling();

	/**
	 * This method is called upon the reception of a new job graph. It gives the plugin the possibility to to rewrite
	 * the job graph before it is processed further.
	 * 
	 * @param jobGraph
	 *        the original job graph
	 * @return the rewritten job graph
	 */
	JobGraph rewriteJobGraph(JobGraph jobGraph);

	/**
	 * This method is called after the initial execution graph has been created from the user's job graph. It gives the
	 * plugin the possibility to rewrite the execution graph before it is processed further or to register to particular
	 * events.
	 * 
	 * @param executionGraph
	 *        the initial execution graph
	 * @return the rewritten execution graph
	 */
	ExecutionGraph rewriteExecutionGraph(ExecutionGraph executionGraph);

	/**
	 * This method is called before the deployment of the execution graph. It provides the plugin with the possibility
	 * to return a custom {@link ProfilingListener} which is then registered with the profiling component. As a result,
	 * the plugin will receive profiling events during the job execution.
	 * 
	 * @param jobID
	 *        the ID of the job to return a profiling listener for
	 * @return the profiling listener for the job or <code>null</code> if the plugin does not want to receive profiling
	 *         data for the job
	 */
	ProfilingListener getProfilingListener(JobID jobID);

	/**
	 * Called by the job manager to indicate that Nephele is about to shut down.
	 */
	void shutdown();
}
