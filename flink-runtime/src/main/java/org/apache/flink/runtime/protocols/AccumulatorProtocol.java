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


package org.apache.flink.runtime.protocols;

import java.io.IOException;

import org.apache.flink.core.protocols.VersionedProtocol;
import org.apache.flink.runtime.accumulators.AccumulatorEvent;
import org.apache.flink.runtime.jobgraph.JobID;

/**
 * The accumulator protocol is implemented by the job manager. TaskManagers can
 * use it to send the collected accumulators and JobClients can use it to get
 * the final accumulator results after the job ended.
 */
public interface AccumulatorProtocol extends VersionedProtocol {

	/**
	 * Report accumulators that were collected in a task. Called by Task
	 * Manager, after the user code was executed but before the task status
	 * update is reported.
	 */
	void reportAccumulatorResult(AccumulatorEvent accumulatorEvent)
			throws IOException;

	/**
	 * Get the final accumulator results. Called by JobClient after the job
	 * ended.
	 */
	AccumulatorEvent getAccumulatorResults(JobID jobID) throws IOException;

}
