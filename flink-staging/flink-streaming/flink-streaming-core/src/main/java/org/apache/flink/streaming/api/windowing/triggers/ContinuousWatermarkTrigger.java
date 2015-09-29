/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.windowing.triggers;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class ContinuousWatermarkTrigger<W extends Window> implements Trigger<Object, W> {
	private static final long serialVersionUID = 1L;

	private long granularity;

	private boolean first = true;

	private ContinuousWatermarkTrigger(long granularity) {
		this.granularity = granularity;
	}

	@Override
	public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) {
		if (first) {
			long start = timestamp - (timestamp % granularity);
			long nextFireTimestamp = start + granularity;

			ctx.registerWatermarkTimer(nextFireTimestamp);
			first = false;
			return TriggerResult.CONTINUE;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onTime(long time, TriggerContext ctx) {
		ctx.registerWatermarkTimer(time + granularity);
		return TriggerResult.FIRE;
	}

	@Override
	public Trigger<Object, W> duplicate() {
		return new ContinuousWatermarkTrigger<>(granularity);
	}

	@Override
	public String toString() {
		return "ContinuousProcessingTimeTrigger(" + granularity + ")";
	}

	@VisibleForTesting
	public long getGranularity() {
		return granularity;
	}

	public static <W extends Window> ContinuousWatermarkTrigger<W> of(long granularity) {
		return new ContinuousWatermarkTrigger<>(granularity);
	}
}
