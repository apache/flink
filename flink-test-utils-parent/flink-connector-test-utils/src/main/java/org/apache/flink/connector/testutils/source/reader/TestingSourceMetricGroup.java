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

package org.apache.flink.connector.testutils.source.reader;

import org.apache.flink.api.connector.source.metrics.SourceMetricGroup;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

/**
 * A testing implementation of {@link SourceMetricGroup}.
 */
public class TestingSourceMetricGroup extends UnregisteredMetricsGroup implements SourceMetricGroup {
	private final Counter numRecordsInCounter = new SimpleCounter();
	private final Counter numBytesInCounter = new SimpleCounter();
	private final Counter numRecordsInErrorsCounter = new SimpleCounter();

	@Override
	public Counter getNumRecordsInCounter() {
		return numRecordsInCounter;
	}

	@Override
	public Counter getNumBytesInCounter() {
		return numBytesInCounter;
	}

	@Override
	public Counter getNumRecordsInErrorsCounter() {
		return numRecordsInErrorsCounter;
	}
}
