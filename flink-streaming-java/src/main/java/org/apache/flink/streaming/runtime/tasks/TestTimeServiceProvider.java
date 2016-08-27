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

package org.apache.flink.streaming.runtime.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

/**
 * This is a {@link TimeServiceProvider} used <b>strictly for testing</b> the
 * processing time functionality.
 * */
public class TestTimeServiceProvider extends TimeServiceProvider {

	private long currentTime = 0;

	private Map<Long, List<Runnable>> registeredTasks = new HashMap<>();

	public void setCurrentTime(long timestamp) {
		this.currentTime = timestamp;

		// decide which timers to fire and put them in a list
		// we do not fire them here to be able to accommodate timers
		// that register other timers. The latter would through an exception.

		Iterator<Map.Entry<Long, List<Runnable>>> it = registeredTasks.entrySet().iterator();
		List<Runnable> toRun = new ArrayList<>();
		while (it.hasNext()) {
			Map.Entry<Long, List<Runnable>> t = it.next();
			if (t.getKey() <= this.currentTime) {
				for (Runnable r: t.getValue()) {
					toRun.add(r);
				}
				it.remove();
			}
		}

		// now do the actual firing.
		for (Runnable r: toRun) {
			r.run();
		}
	}

	@Override
	public long getCurrentProcessingTime() {
		return currentTime;
	}

	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, Runnable target) {
		List<Runnable> tasks = registeredTasks.get(timestamp);
		if (tasks == null) {
			tasks = new ArrayList<>();
			registeredTasks.put(timestamp, tasks);
		}
		tasks.add(target);
		return null;
	}

	public int getNoOfRegisteredTimers() {
		int count = 0;
		for (List<Runnable> tasks: registeredTasks.values()) {
			count += tasks.size();
		}
		return count;
	}

	@Override
	public void shutdownService() throws Exception {
		this.registeredTasks.clear();
		this.registeredTasks = null;
	}
}
