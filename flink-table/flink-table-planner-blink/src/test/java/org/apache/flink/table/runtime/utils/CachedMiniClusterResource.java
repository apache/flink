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

package org.apache.flink.table.runtime.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.rules.ExternalResource;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Mini clusters cache to share cluster between Tests. This class use async thread to
 * clean the expired cluster.
 *
 * <p>NOTE: Do not use {@link MiniClusterWithClientResource}, it will pollute thread local
 * in {@link StreamExecutionEnvironment}.
 */
public class CachedMiniClusterResource extends ExternalResource {

	public static final int DEFAULT_PARALLELISM = 4;
	private static final Long CLEAN_INTERVAL_MILLS = 15 * 1000L;

	private static final LinkedBlockingQueue<Tuple2<Long, MiniClusterResource>> CACHE = new LinkedBlockingQueue<>();
	private static final ReadWriteLock LOCK = new ReentrantReadWriteLock();
	private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(
			1, new ExecutorThreadFactory("CachedMiniClusterResource"));
	static {
		SCHEDULER.scheduleWithFixedDelay(
				CachedMiniClusterResource::clean,
				CLEAN_INTERVAL_MILLS, CLEAN_INTERVAL_MILLS, TimeUnit.MILLISECONDS);
	}

	private static MiniClusterResource loadCluster() throws Exception {
		MiniClusterResource cluster = new MiniClusterResource(
				new MiniClusterResourceConfiguration.Builder()
						.setConfiguration(getConfiguration())
						.setNumberTaskManagers(1)
						.setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
						.build());

		cluster.before();
		return cluster;
	}

	private static Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "100m");
		return config;
	}

	private static void clean() {
		try {
			LOCK.writeLock().lock();
			Iterator<Tuple2<Long, MiniClusterResource>> iter = CACHE.iterator();
			while (iter.hasNext()) {
				Tuple2<Long, MiniClusterResource> tuple2 = iter.next();
				if (System.currentTimeMillis() - tuple2.f0 > CLEAN_INTERVAL_MILLS) {
					tuple2.f1.after();
					iter.remove();
				}
			}
		} finally {
			LOCK.writeLock().unlock();
		}
	}

	private MiniClusterResource cluster;

	@Override
	protected void before() throws Throwable {
		try {
			LOCK.readLock().lock();
			Tuple2<Long, MiniClusterResource> tuple2 = CACHE.poll();
			cluster = tuple2 == null ? loadCluster() : tuple2.f1;
		} finally {
			LOCK.readLock().unlock();
		}
	}

	@Override
	protected void after() {
		try {
			LOCK.readLock().lock();
			CACHE.add(new Tuple2<>(System.currentTimeMillis(), cluster));
		} finally {
			LOCK.readLock().unlock();
		}
	}

	public MiniClusterResource getCluster() {
		return cluster;
	}
}
