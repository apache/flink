/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmanager.accumulators;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.util.SerializedValue;

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

	/** Map of accumulators belonging to recently started jobs */
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
			
			JobAccumulators jobAccumulators = this.jobAccumulators.get(jobID);
			if (jobAccumulators == null) {
				jobAccumulators = new JobAccumulators();
				this.jobAccumulators.put(jobID, jobAccumulators);
				cleanup(jobID);
			}
			jobAccumulators.processNew(newAccumulators);
		}
	}

	public Map<String, Object> getJobAccumulatorResults(JobID jobID) {
		Map<String, Object> result = new HashMap<String, Object>();

		JobAccumulators acc;
		synchronized (jobAccumulators) {
			acc = jobAccumulators.get(jobID);
		}

		if (acc != null) {
			for (Map.Entry<String, Accumulator<?, ?>> entry : acc.getAccumulators().entrySet()) {
				result.put(entry.getKey(), entry.getValue().getLocalValue());
			}
		}

		return result;
	}

	public Map<String, SerializedValue<Object>> getJobAccumulatorResultsSerialized(JobID jobID) throws IOException {
		JobAccumulators acc;
		synchronized (jobAccumulators) {
			acc = jobAccumulators.get(jobID);
		}

		if (acc == null || acc.getAccumulators().isEmpty()) {
			return Collections.emptyMap();
		}

		Map<String, SerializedValue<Object>> result = new HashMap<String, SerializedValue<Object>>();
		for (Map.Entry<String, Accumulator<?, ?>> entry : acc.getAccumulators().entrySet()) {
			result.put(entry.getKey(), new SerializedValue<Object>(entry.getValue().getLocalValue()));
		}

		return result;
	}

	public StringifiedAccumulatorResult[] getJobAccumulatorResultsStringified(JobID jobID) throws IOException {
		JobAccumulators acc;
		synchronized (jobAccumulators) {
			acc = jobAccumulators.get(jobID);
		}

		if (acc == null || acc.getAccumulators().isEmpty()) {
			return new StringifiedAccumulatorResult[0];
		}

		Map<String, Accumulator<?, ?>> accMap = acc.getAccumulators();

		StringifiedAccumulatorResult[] result = new StringifiedAccumulatorResult[accMap.size()];
		int i = 0;
		for (Map.Entry<String, Accumulator<?, ?>> entry : accMap.entrySet()) {
			String type = entry.getValue() == null ? "(null)" : entry.getValue().getClass().getSimpleName();
			String value = entry.getValue() == null ? "(null)" : entry.getValue().toString();
			result[i++] = new StringifiedAccumulatorResult(entry.getKey(), type, value);
		}
		return result;
	}

	/**
	 * Cleanup data for the oldest jobs if the maximum number of entries is reached.
	 *
	 * @param jobId The (potentially new) JobId.
	 */
	private void cleanup(JobID jobId) {
		if (!lru.contains(jobId)) {
			lru.addFirst(jobId);
		}
		if (lru.size() > this.maxEntries) {
			JobID toRemove = lru.removeLast();
			this.jobAccumulators.remove(toRemove);
		}
	}
}
