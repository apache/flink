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

package eu.stratosphere.nephele.profiling;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This interface must be implemented by profiling components
 * for the job manager.
 * 
 * @author warneke
 */
public interface JobManagerProfiler {

	/**
	 * Registers the given {@link ExecutionGraph} for profiling.
	 * 
	 * @param executionGraph
	 *        the {@link ExecutionGraph} to register for profiling
	 */
	void registerProfilingJob(ExecutionGraph executionGraph);

/**
	 * Unregisters the given {@link ExecutionGraph from profiling. Calling this
	 * method will also unregister all of the job's registered listeners.
	 * 
	 * @param executionGraph the {@link ExecutionGraph} to unregister.
	 */
	void unregisterProfilingJob(ExecutionGraph executionGraph);

	/**
	 * Registers the given {@link ProfilingListener} object to receive
	 * profiling data for the job with the given job ID.
	 * 
	 * @param jobID
	 *        the ID of the job to receive profiling data for
	 * @param profilingListener
	 *        the {@link ProfilingListener} object to register
	 */
	void registerForProfilingData(JobID jobID, ProfilingListener profilingListener);

	/**
	 * Unregisters the given {@link ProfilingListener} object from receiving
	 * profiling data issued by the job manager's profiling component.
	 * 
	 * @param jobID
	 *        the ID of the job the {@link ProfilingListener} object has originally been registered for
	 * @param profilingListener
	 *        the {@link ProfilingListener} object to unregister
	 */
	void unregisterFromProfilingData(JobID jobID, ProfilingListener profilingListener);

	/**
	 * Shuts done the job manager's profiling component
	 * and stops all its internal processes.
	 */
	void shutdown();
}
