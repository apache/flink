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

package org.apache.flink.api.connector.source.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/**
 * An interface that defines the metric names of the Source (FLIP-27). The interface
 * also exposes the methods for the predefined metrics that need to be updated by
 * the Source implementations.
 */
public interface SourceMetricGroup extends MetricGroup {
	// Source metrics convention specified in FLIP-33.
	String CURRENT_FETCH_EVENT_TIME_LAG = "currentFetchEventTimeLag";
	String CURRENT_EMIT_EVENT_TIME_LAG = "currentEmitEventTimeLag";
	String WATERMARK_LAG = "watermarkLag";
	String SOURCE_IDLE_TIME = "sourceIdleTime";
	String PENDING_BYTES = "pendingBytes";
	String PENDING_RECORDS = "pendingRecords";
	String IO_NUM_RECORDS_IN_ERRORS = "numRecordsInErrors";

	/**
	 * @return the counter to record number of records received by the SourceReader.
	 */
	Counter getNumRecordsInCounter();

	/**
	 * @return the counter to record number of bytes received by the SourceReader.
	 */
	Counter getNumBytesInCounter();

	/**
	 * @return the counter to record the errors encountered when reading the records.
	 */
	Counter getNumRecordsInErrorsCounter();

}
