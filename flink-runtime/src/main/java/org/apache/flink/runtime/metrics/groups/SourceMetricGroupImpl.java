/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.metrics.SourceMetricGroup;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.clock.SystemClock;

import java.util.function.Supplier;

/**
 * A metric group that adds source specific metrics to the SourceOperator.
 * It adds Source specific metrics to the OperatorMetricsGroup.
 */
public class SourceMetricGroupImpl extends ProxyMetricGroup<OperatorMetricGroup> implements SourceMetricGroup {
	private final Counter numRecordsInCounter;
	private final Counter numRecordsInErrorsCounter;
	private final Counter numBytesIn;
	private final Supplier<Long> currentWatermarkSupplier;

	public SourceMetricGroupImpl(
			OperatorMetricGroup operatorMetricGroup,
			Counter numBytesInCounter,
			Supplier<Long> currentWatermarkSupplier) {
		super(operatorMetricGroup);
		this.numRecordsInCounter = operatorMetricGroup.getIOMetricGroup().getNumRecordsInCounter();
		this.numRecordsInErrorsCounter = operatorMetricGroup.counter(SourceMetricGroup.IO_NUM_RECORDS_IN_ERRORS);
		this.numBytesIn = numBytesInCounter;
		this.currentWatermarkSupplier = currentWatermarkSupplier;
		operatorMetricGroup.gauge(
				SourceMetricGroup.WATERMARK_LAG,
				() -> {
					long currentWatermark = currentWatermarkSupplier.get();
					if (currentWatermark < 0) {
						return -1L;
					} else {
						return SystemClock.getInstance().absoluteTimeMillis() - currentWatermark;
					}
				});
	}

	@Override
	public Counter getNumRecordsInCounter() {
		return numRecordsInCounter;
	}

	public Counter getNumBytesInCounter() {
		return numBytesIn;
	}

	public Counter getNumRecordsInErrorsCounter() {
		return numRecordsInErrorsCounter;
	}

	@VisibleForTesting
	public Supplier<Long> getCurrentWatermarkSupplier() {
		return currentWatermarkSupplier;
	}
}
