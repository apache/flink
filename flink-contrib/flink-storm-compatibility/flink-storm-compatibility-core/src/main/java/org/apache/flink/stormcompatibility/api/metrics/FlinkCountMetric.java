/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.stormcompatibility.api.metrics;

import backtype.storm.metric.api.CountMetric;
import org.apache.flink.api.common.accumulators.LongCounter;

/**
 * The wrapper for using {@link backtype.storm.metric.api.CountMetric}
 */
public class FlinkCountMetric extends CountMetric {

	private LongCounter longCounter;

	public FlinkCountMetric () {
	}

	public void setCounter (LongCounter longCounter) {
		this.longCounter = longCounter;
	}

	@Override
	public void incr() {
		incrBy(1);
	}

	@Override
	public void incrBy(long incrementBy) {
		this.longCounter.add(incrementBy);
	}

	@Override
	public Object getValueAndReset() {
		long ret = this.longCounter.getLocalValue();
		this.longCounter.resetLocal();
		return ret;
	}
}
