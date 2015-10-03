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

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * A {@link Trigger} that fires once the watermark passes the end of the window
 * to which a pane belongs.
 *
 * @see org.apache.flink.streaming.api.watermark.Watermark
 */
public class WatermarkTrigger implements Trigger<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	boolean isFirst = true;

	private WatermarkTrigger() {}

	@Override
	public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
		if (isFirst) {
			ctx.registerWatermarkTimer(window.maxTimestamp());
			isFirst = false;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onTime(long time, TriggerContext ctx) {
		return TriggerResult.FIRE_AND_PURGE;
	}

	@Override
	public Trigger<Object, TimeWindow> duplicate() {
		return new WatermarkTrigger();
	}

	@Override
	public String toString() {
		return "WatermarkTrigger()";
	}

	/**
	 * Creates trigger that fires once the watermark passes the end of the window.
	 */
	public static WatermarkTrigger create() {
		return new WatermarkTrigger();
	}

}
