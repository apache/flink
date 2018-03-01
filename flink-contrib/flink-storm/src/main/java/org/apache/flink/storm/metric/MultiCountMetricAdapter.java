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

package org.apache.flink.storm.metric;

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import org.apache.storm.metric.api.MultiCountMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * @since Mar 5, 2017
 */
public class MultiCountMetricAdapter extends MultiCountMetric {

	private Map<String, CounterMetricAdapter> counterPerKey = new HashMap<>();
	private StreamingRuntimeContext context;

	public MultiCountMetricAdapter(final StreamingRuntimeContext context) {
		this.context = context;
	}

	public CounterMetricAdapter scope(final String key) {
		CounterMetricAdapter val = counterPerKey.get(key);
		if (val == null) {
			Counter flinkCounter = context.getMetricGroup().counter(key);
			val = new CounterMetricAdapter(flinkCounter);
			counterPerKey.put(key, val);
		}
		return val;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Object getValueAndReset() {
		Map ret = new HashMap();
		for (Map.Entry<String, CounterMetricAdapter> e : counterPerKey.entrySet()) {
			ret.put(e.getKey(), e.getValue().getValueAndReset());
		}
		return ret;
	}

}
