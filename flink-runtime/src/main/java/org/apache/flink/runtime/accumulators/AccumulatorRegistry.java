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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Main accumulator registry which encapsulates internal and user-defined accumulators.
 */
public class AccumulatorRegistry {

	protected static final Logger LOG = LoggerFactory.getLogger(AccumulatorRegistry.class);

	protected final JobID jobID;
	protected final ExecutionAttemptID taskID;

	/* Flink's internal Accumulator values stored for the executing task. */
	private final Map<Metric, Accumulator<?, ?>> flinkAccumulators =
			new HashMap<Metric, Accumulator<?, ?>>();

	/* User-defined Accumulator values stored for the executing task. */
	private final Map<String, Accumulator<?, ?>> userAccumulators =
			Collections.synchronizedMap(new HashMap<String, Accumulator<?, ?>>());

	/* The reporter reference that is handed to the reporting tasks. */
	private final ReadWriteReporter reporter;

	/**
	 * Flink metrics supported
	 */
	public enum Metric {
		NUM_RECORDS_IN,
		NUM_RECORDS_OUT,
		NUM_BYTES_IN,
		NUM_BYTES_OUT
	}


	public AccumulatorRegistry(JobID jobID, ExecutionAttemptID taskID) {
		this.jobID = jobID;
		this.taskID = taskID;
		this.reporter = new ReadWriteReporter(flinkAccumulators);
	}

	/**
	 * Creates a snapshot of this accumulator registry.
	 * @return a serialized accumulator map
	 */
	public AccumulatorSnapshot getSnapshot() {
		try {
			return new AccumulatorSnapshot(jobID, taskID, flinkAccumulators, userAccumulators);
		} catch (IOException e) {
			LOG.warn("Failed to serialize accumulators for task.", e);
			return null;
		}
	}

	/**
	 * Gets the map for user-defined accumulators.
	 */
	public Map<String, Accumulator<?, ?>> getUserMap() {
		return userAccumulators;
	}

	/**
	 * Gets the reporter for flink internal metrics.
	 */
	public Reporter getReadWriteReporter() {
		return reporter;
	}

	/**
	 * Interface for Flink's internal accumulators.
	 */
	public interface Reporter {
		void reportNumRecordsIn(long value);
		void reportNumRecordsOut(long value);
		void reportNumBytesIn(long value);
		void reportNumBytesOut(long value);
	}

	/**
	 * Accumulator based reporter for keeping track of internal metrics (e.g. bytes and records in/out)
	 */
	private static class ReadWriteReporter implements Reporter {

		private LongCounter numRecordsIn = new LongCounter();
		private LongCounter numRecordsOut = new LongCounter();
		private LongCounter numBytesIn = new LongCounter();
		private LongCounter numBytesOut = new LongCounter();

		private ReadWriteReporter(Map<Metric, Accumulator<?,?>> accumulatorMap) {
			accumulatorMap.put(Metric.NUM_RECORDS_IN, numRecordsIn);
			accumulatorMap.put(Metric.NUM_RECORDS_OUT, numRecordsOut);
			accumulatorMap.put(Metric.NUM_BYTES_IN, numBytesIn);
			accumulatorMap.put(Metric.NUM_BYTES_OUT, numBytesOut);
		}

		@Override
		public void reportNumRecordsIn(long value) {
			numRecordsIn.add(value);
		}

		@Override
		public void reportNumRecordsOut(long value) {
			numRecordsOut.add(value);
		}

		@Override
		public void reportNumBytesIn(long value) {
			numBytesIn.add(value);
		}

		@Override
		public void reportNumBytesOut(long value) {
			numBytesOut.add(value);
		}
	}

}
