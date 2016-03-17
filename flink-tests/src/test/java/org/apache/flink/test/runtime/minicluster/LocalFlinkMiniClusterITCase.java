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

package org.apache.flink.test.runtime.minicluster;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.junit.Test;
import scala.concurrent.ExecutionContext$;
import scala.concurrent.forkjoin.ForkJoinPool;
import scala.concurrent.impl.ExecutionContextImpl;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.fail;

public class LocalFlinkMiniClusterITCase {

	private static String[] ALLOWED_THREAD_PREFIXES = { };

	@Test
	public void testLocalFlinkMiniClusterWithMultipleTaskManagers() {
		
		final ActorSystem system = ActorSystem.create("Testkit", AkkaUtils.getDefaultAkkaConfig());
		LocalFlinkMiniCluster miniCluster = null;

		final int numTMs = 3;
		final int numSlots = 14;

		// gather the threads that already exist
		final Set<Thread> threadsBefore = new HashSet<>();
		{
			final Thread[] allThreads = new Thread[Thread.activeCount()];
			Thread.enumerate(allThreads);
			threadsBefore.addAll(Arrays.asList(allThreads));
		}
		
		
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTMs);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots);
			miniCluster = new LocalFlinkMiniCluster(config, true);

			miniCluster.start();

			final ActorGateway jmGateway = miniCluster.getLeaderGateway(TestingUtils.TESTING_DURATION());

			new JavaTestKit(system) {{
				final ActorGateway selfGateway = new AkkaActorGateway(getRef(), null);

				new Within(TestingUtils.TESTING_DURATION()) {

					@Override
					protected void run() {
						jmGateway.tell(
								JobManagerMessages.getRequestNumberRegisteredTaskManager(),
								selfGateway);

						expectMsgEquals(TestingUtils.TESTING_DURATION(), numTMs);

						jmGateway.tell(
								JobManagerMessages.getRequestTotalNumberOfSlots(),
								selfGateway);

						expectMsgEquals(TestingUtils.TESTING_DURATION(), numTMs*numSlots);
					}
				};
			}};


		} finally {
			if (miniCluster != null) {
				miniCluster.stop();
				miniCluster.awaitTermination();
			}

			JavaTestKit.shutdownActorSystem(system);
			system.awaitTermination();
		}

		// shut down the global execution context, to make sure it does not affect this testing
		try {
			Field f = ExecutionContextImpl.class.getDeclaredField("executor");
			f.setAccessible(true);
			
			Object exec = ExecutionContext$.MODULE$.global();
			ForkJoinPool executor = (ForkJoinPool) f.get(exec);
			executor.shutdownNow();
		}
		catch (Exception e) {
			System.err.println("Cannot test proper thread shutdown for local execution.");
			return;
		}
		
		// check for remaining threads
		// we need to check repeatedly for a while, because some threads shut down slowly
		
		long deadline = System.currentTimeMillis() + 30000;
		boolean foundThreads = true;
		String threadName = "";
		
		while (System.currentTimeMillis() < deadline) {
			// check that no additional threads remain
			final Thread[] threadsAfter = new Thread[Thread.activeCount()];
			Thread.enumerate(threadsAfter);

			foundThreads = false;
			for (Thread t : threadsAfter) {
				if (t.isAlive() && !threadsBefore.contains(t)) {
					// this thread was not there before. check if it is allowed
					boolean allowed = false;
					for (String prefix : ALLOWED_THREAD_PREFIXES) {
						if (t.getName().startsWith(prefix)) {
							allowed = true;
							break;
						}
					}
					
					if (!allowed) {
						foundThreads = true;
						threadName = t.toString();
						break;
					}
				}
			}
			
			if (foundThreads) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException ignored) {}
			} else {
				break;
			}
		}
		
		if (foundThreads) {
			fail("Thread " + threadName + " was started by the mini cluster, but not shut down");
		}
	}
}
