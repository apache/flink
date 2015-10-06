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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that assigns all elements to the same global window.
 *
 * <p>
 * Use this if you want to use a {@link Trigger} and
 * {@link org.apache.flink.streaming.api.windowing.evictors.Evictor} to to flexible, policy based
 * windows.
 */
public class GlobalWindows extends WindowAssigner<Object, GlobalWindow> {
	private static final long serialVersionUID = 1L;

	private GlobalWindows() {}

	@Override
	public Collection<GlobalWindow> assignWindows(Object element, long timestamp) {
		return Collections.singletonList(GlobalWindow.get());
	}

	@Override
	public Trigger<Object, GlobalWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return null;
	}

	@Override
	public String toString() {
		return "GlobalWindows()";
	}

	/**
	 * Creates a new {@code GlobalWindows} {@link WindowAssigner} that assigns
	 * all elements to the same {@link GlobalWindow}.
	 *
	 * @return The global window policy.
	 */
	public static GlobalWindows create() {
		return new GlobalWindows();
	}
}
