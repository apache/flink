/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics;

import org.apache.flink.metrics.View;

import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.metrics.View.UPDATE_INTERVAL_SECONDS;

/**
 * The ViewUpdater is responsible for updating all metrics that implement the {@link View} interface.
 */
public class ViewUpdater {
	private final Set<View> toAdd = new HashSet<>();
	private final Set<View> toRemove = new HashSet<>();

	private final Object lock = new Object();

	public ViewUpdater(ScheduledExecutorService executor) {
		executor.scheduleWithFixedDelay(new ViewUpdaterTask(lock, toAdd, toRemove), 5, UPDATE_INTERVAL_SECONDS, TimeUnit.SECONDS);
	}

	/**
	 * Notifies this ViewUpdater of a new metric that should be regularly updated.
	 *
	 * @param view metric that should be regularly updated
	 */
	public void notifyOfAddedView(View view) {
		synchronized (lock) {
			toAdd.add(view);
		}
	}

	/**
	 * Notifies this ViewUpdater of a metric that should no longer be regularly updated.
	 *
	 * @param view metric that should no longer be regularly updated
	 */
	public void notifyOfRemovedView(View view) {
		synchronized (lock) {
			toRemove.add(view);
		}
	}

	/**
	 * The TimerTask doing the actual updating.
	 */
	private static class ViewUpdaterTask extends TimerTask {
		private final Object lock;
		private final Set<View> views;
		private final Set<View> toAdd;
		private final Set<View> toRemove;

		private ViewUpdaterTask(Object lock, Set<View> toAdd, Set<View> toRemove) {
			this.lock = lock;
			this.views = new HashSet<>();
			this.toAdd = toAdd;
			this.toRemove = toRemove;
		}

		@Override
		public void run() {
			for (View toUpdate : this.views) {
				toUpdate.update();
			}

			synchronized (lock) {
				views.addAll(toAdd);
				toAdd.clear();
				views.removeAll(toRemove);
				toRemove.clear();
			}
		}
	}
}
