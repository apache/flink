/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobmanager.accumulators;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class manages the accumulators for different jobs. Either the jobs are
 * running and new accumulator results have to be merged in, or the jobs are no
 * longer running and the results shall be still available for the client or the
 * web interface. Accumulators for older jobs are automatically removed when new
 * arrive, based on a maximum number of entries.
 * 
 * All functions are thread-safe and thus can be called directly from
 * JobManager.
 */
public class AccumulatorManager {

	// Map of accumulators belonging to recently started jobs
	private final Map<JobID, JobAccumulators> jobAccumulators = new HashMap<JobID, JobAccumulators>();

	private final LinkedList<JobID> lru = new LinkedList<JobID>();
	private int maxEntries;

	public AccumulatorManager(int maxEntries) {
		this.maxEntries = maxEntries;
	}

	/**
	 * Merges the new accumulators with the existing accumulators collected for
	 * the job.
	 */
	public void processIncomingAccumulators(JobID jobID,
			Map<String, Accumulator<?, ?>> newAccumulators) {
		synchronized (this.jobAccumulators) {
			
//			System.out.println("JobManager: Received accumulator result for job "
//					+ jobID.toString());
//			System.out.println(AccumulatorHelper.getAccumulatorsFormated(newAccumulators));
			
			JobAccumulators jobAccumulators = this.jobAccumulators.get(jobID);
			if (jobAccumulators == null) {
				jobAccumulators = new JobAccumulators();
				this.jobAccumulators.put(jobID, jobAccumulators);
				cleanup(jobID);
			}
			jobAccumulators.processNew(newAccumulators);
		}
	}

	/**
	 * Returns all collected accumulators for the job. For efficiency the
	 * internal accumulator is returned, so please use it read-only.
	 */
	public Map<String, Accumulator<?, ?>> getJobAccumulators(JobID jobID) {
		JobAccumulators jobAccumulators = this.jobAccumulators.get(jobID);
		if (jobAccumulators == null) {
			return new HashMap<String, Accumulator<?, ?>>();
		}
		return jobAccumulators.getAccumulators();
	}

	/**
	 * Cleanup data for the oldest jobs if the maximum number of entries is
	 * reached.
	 */
	private void cleanup(JobID jobId) {
		if (!lru.contains(jobId))
			lru.addFirst(jobId);
		if (lru.size() > this.maxEntries) {
			JobID toRemove = lru.removeLast();
			this.jobAccumulators.remove(toRemove);
		}
	}
}
