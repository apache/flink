/*
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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * A {@link Trigger} that fires once the current system time passes the end of the window
 * to which a pane belongs.
 */
@PublicEvolving
public class ProcessingTimeTrigger extends Trigger<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private ProcessingTimeTrigger() {}

	@Override
	public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
		ctx.registerProcessingTimeTimer(window.maxTimestamp());
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
		return TriggerResult.FIRE;
	}

	@Override
	public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
		ctx.deleteProcessingTimeTimer(window.maxTimestamp());
	}

	@Override
	public boolean canMerge() {
		return true;
	}

	@Override
	public void onMerge(TimeWindow window,
			OnMergeContext ctx) {
		// only register a timer if the time is not yet past the end of the merged window
		// this is in line with the logic in onElement(). If the time is past the end of
		// the window onElement() will fire and setting a timer here would fire the window twice.
		long windowMaxTimestamp = window.maxTimestamp();
		if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
			ctx.registerProcessingTimeTimer(windowMaxTimestamp);
		}
	}

	@Override
	public String toString() {
		return "ProcessingTimeTrigger()";
	}

	/**
	 * Creates a new trigger that fires once system time passes the end of the window.
	 */
	public static ProcessingTimeTrigger create() {
		return new ProcessingTimeTrigger();
	}

}
