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

package org.apache.flink.table.temptable;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;

/**
 * Handle metrics for table service.
 */
public class TableServiceMetrics {

	public static final String WRITE_TOTAL_BYTES_METRIC = "tableServiceWriteTotalBytes";

	public static final String READ_TOTAL_BYTES_METRIC = "tableServiceReadTotalBytes";

	public static final String READ_BPS_METRIC = "tableServiceReadBps";

	public static final String WRITE_BPS_METRIC = "tableServiceWriteBps";

	private Meter readBpsMetrics;

	private Counter readTotalBytesMetrics;

	private Meter writeBpsMetrics;

	private Counter writeTotalBytesMetrics;

	private final MetricGroup metricGroup;

	public TableServiceMetrics(MetricGroup metricGroup) {
		this.metricGroup = metricGroup;
		writeTotalBytesMetrics = metricGroup.counter(WRITE_TOTAL_BYTES_METRIC, new SimpleCounter());
		writeBpsMetrics = metricGroup.meter(WRITE_BPS_METRIC, new MeterView(writeTotalBytesMetrics, 10));
		readTotalBytesMetrics = metricGroup.counter(READ_TOTAL_BYTES_METRIC, new SimpleCounter());
		readBpsMetrics = metricGroup.meter(READ_BPS_METRIC, new MeterView(readTotalBytesMetrics, 10));
	}

	public Counter getReadTotalBytesMetrics() {
		return readTotalBytesMetrics;
	}

	public Counter getWriteTotalBytesMetrics() {
		return writeTotalBytesMetrics;
	}
}
