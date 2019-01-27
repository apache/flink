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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.pattern.Patterns;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.fail;

/**
 * This test verifies the behavior of the recovery in the case when a TaskManager
 * fails (shut down) in the middle of a job execution.
 *
 * <p>The test works with multiple in-process task managers. Initially, it starts a JobManager
 * and two TaskManagers with 2 slots each. It submits a program with parallelism 4
 * and waits until all tasks are brought up (coordination between the test and the tasks
 * happens via shared blocking queues). It then starts another TaskManager, which is
 * guaranteed to remain empty (all tasks are already deployed) and kills one of
 * the original task managers. The recovery should restart the tasks on the new TaskManager.
 */
@SuppressWarnings("serial")
public class TaskManagerFailureRecoveryITCase extends TestLogger {

	@Test
	public void testRestartWithFailingTaskManager() {

		final int parallelism = 4;

		LocalFlinkMiniCluster cluster = null;
		ActorSystem additionalSystem = null;

		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
			config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);
			config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 16L);

			config.setString(AkkaOptions.WATCH_HEARTBEAT_INTERVAL, "500 ms");
			config.setString(AkkaOptions.WATCH_HEARTBEAT_PAUSE, "20 s");
			config.setInteger(AkkaOptions.WATCH_THRESHOLD, 20);

			cluster = new LocalFlinkMiniCluster(config, false);

			cluster.start();

			// for the result
			List<Long> resultCollection = new ArrayList<Long>();

			final ExecutionEnvironment env = new TestEnvironment(cluster, parallelism, false);

			env.setParallelism(parallelism);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000));
			env.getConfig().disableSysoutLogging();

			env.generateSequence(1, 10)
					.map(new FailingMapper<Long>())
					.reduce(new ReduceFunction<Long>() {
						@Override
						public Long reduce(Long value1, Long value2) {
							return value1 + value2;
						}
					})
					.output(new LocalCollectionOutputFormat<Long>(resultCollection));

			// simple reference (atomic does not matter) to pass back an exception from the trigger thread
			final AtomicReference<Throwable> ref = new AtomicReference<Throwable>();

			// trigger the execution from a separate thread, so we are available to temper with the
			// cluster during the execution
			Thread trigger = new Thread("program trigger") {
				@Override
				public void run() {
					try {
						env.execute();
					}
					catch (Throwable t) {
						ref.set(t);
					}
				}
			};
			trigger.setDaemon(true);
			trigger.start();

			// block until all the mappers are actually deployed
			// the mappers in turn are waiting
			for (int i = 0; i < parallelism; i++) {
				FailingMapper.TASK_TO_COORD_QUEUE.take();
			}

			// bring up one more task manager and wait for it to appear
			{
				additionalSystem = cluster.startTaskManagerActorSystem(2);
				ActorRef additionalTaskManager = cluster.startTaskManager(2, additionalSystem);
				Object message = TaskManagerMessages.getNotifyWhenRegisteredAtJobManagerMessage();
				Future<Object> future = Patterns.ask(additionalTaskManager, message, 30000);

				try {
					Await.result(future, new FiniteDuration(30000, TimeUnit.MILLISECONDS));
				}
				catch (TimeoutException e) {
					fail ("The additional TaskManager did not come up within 30 seconds");
				}
			}

			// kill the two other TaskManagers
			for (ActorRef tm : cluster.getTaskManagersAsJava()) {
				tm.tell(PoisonPill.getInstance(), null);
			}

			// wait for the next set of mappers (the recovery ones) to come online
			for (int i = 0; i < parallelism; i++) {
				FailingMapper.TASK_TO_COORD_QUEUE.take();
			}

			// tell the mappers that they may continue this time
			for (int i = 0; i < parallelism; i++) {
				FailingMapper.COORD_TO_TASK_QUEUE.add(new Object());
			}

			// wait for the program to finish
			trigger.join();
			if (ref.get() != null) {
				Throwable t = ref.get();
				t.printStackTrace();
				fail("Program execution caused an exception: " + t.getMessage());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (additionalSystem != null) {
				additionalSystem.shutdown();
			}
			if (cluster != null) {
				cluster.stop();
			}
		}
	}

	private static class FailingMapper<T> extends RichMapFunction<T, T> {
		private static final long serialVersionUID = 4435412404173331157L;

		private static final BlockingQueue<Object> TASK_TO_COORD_QUEUE = new LinkedBlockingQueue<Object>();

		private static final BlockingQueue<Object> COORD_TO_TASK_QUEUE = new LinkedBlockingQueue<Object>();

		@Override
		public void open(Configuration parameters) throws Exception {
			TASK_TO_COORD_QUEUE.add(new Object());
			COORD_TO_TASK_QUEUE.take();
		}

		@Override
		public T map(T value) throws Exception {
			return value;
		}
	}
}
