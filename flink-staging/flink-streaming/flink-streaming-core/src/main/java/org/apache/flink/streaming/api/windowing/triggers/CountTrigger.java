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

import org.apache.flink.streaming.api.windowing.windows.Window;

public class CountTrigger<W extends Window> implements Trigger<Object, W> {
	private static final long serialVersionUID = 1L;

	private long maxCount;
	private long count;

	private CountTrigger(long maxCount) {
		this.maxCount = maxCount;
		count = 0;
	}

	@Override
	public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) {
		count++;
		if (count >= maxCount) {
			count = 0;
			return TriggerResult.FIRE;
		}
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onTime(long time, TriggerContext ctx) {
		return null;
	}

	@Override
	public Trigger<Object, W> duplicate() {
		return new CountTrigger<>(maxCount);
	}

	@Override
	public String toString() {
		return "CountTrigger(" +  maxCount + ")";
	}

	public static <W extends Window> CountTrigger<W> of(long maxCount) {
		return new CountTrigger<>(maxCount);
	}
}
