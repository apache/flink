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
package org.apache.flink.streaming.api.windowing.assigners;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.windowing.time.AbstractTime;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.List;

public class SlidingProcessingTimeWindows extends WindowAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private final long size;

	private final long slide;

	private transient List<TimeWindow> result;

	private SlidingProcessingTimeWindows(long size, long slide) {
		this.size = size;
		this.slide = slide;
		this.result = Lists.newArrayListWithCapacity((int) (size / slide));
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		this.result = Lists.newArrayListWithCapacity((int) (size / slide));
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
		result.clear();
		long time = System.currentTimeMillis();
		long lastStart = time - time % slide;
		for (long start = lastStart;
			start > time - size;
			start -= slide) {
			result.add(new TimeWindow(start, size));
		}
		return result;
	}

	public long getSize() {
		return size;
	}

	public long getSlide() {
		return slide;
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger() {
		return ProcessingTimeTrigger.create();
	}

	@Override
	public String toString() {
		return "SlidingProcessingTimeWindows(" + size + ", " + slide + ")";
	}

	/**
	 * Creates a new {@code SlidingProcessingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to sliding time windows based on the current processing time.
	 *
	 * @param size The size of the generated windows.
	 * @param slide The slide interval of the generated windows.
	 * @return The time policy.
	 */
	public static SlidingProcessingTimeWindows of(AbstractTime size, AbstractTime slide) {
		return new SlidingProcessingTimeWindows(size.toMilliseconds(), slide.toMilliseconds());
	}
}
