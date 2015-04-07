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

package org.apache.flink.runtime.accumulators;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.util.SerializedValue;

/**
 * This class encapsulates a map of accumulators for a single job. It is used
 * for the transfer from TaskManagers to the JobManager and from the JobManager
 * to the Client.
 */
public class AccumulatorEvent extends SerializedValue<Map<String, Accumulator<?, ?>>> {

	private static final long serialVersionUID = 8965894516006882735L;

	/** JobID for the target job */
	private final JobID jobID;


	public AccumulatorEvent(JobID jobID, Map<String, Accumulator<?, ?>> accumulators) throws IOException {
		super(accumulators);
		this.jobID = jobID;
	}

	public JobID getJobID() {
		return this.jobID;
	}
}
