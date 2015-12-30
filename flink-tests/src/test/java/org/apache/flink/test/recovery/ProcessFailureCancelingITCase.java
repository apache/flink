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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.util.Timeout;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.NetUtils;

import org.junit.Test;

import scala.Some;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getCurrentClasspath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test makes sure that jobs are canceled properly in cases where
 * the task manager went down and did not respond to cancel messages.
 */
@SuppressWarnings("serial")
public class ProcessFailureCancelingITCase {
	
	@Test
	public void testCancelingOnProcessFailure() {
		final StringWriter processOutput = new StringWriter();

		ActorSystem jmActorSystem = null;
		Process taskManagerProcess = null;
		
		try {
			// check that we run this test only if the java command
			// is available on this machine
			String javaCommand = getJavaCommandPath();
			if (javaCommand == null) {
				System.out.println("---- Skipping Process Failure test : Could not find java executable ----");
				return;
			}

			// create a logging file for the process
			File tempLogFile = File.createTempFile(getClass().getSimpleName() + "-", "-log4j.properties");
			tempLogFile.deleteOnExit();
			CommonTestUtils.printLog4jDebugConfig(tempLogFile);

			// find a free port to start the JobManager
			final int jobManagerPort = NetUtils.getAvailablePort();

			// start a JobManager
			Tuple2<String, Object> localAddress = new Tuple2<String, Object>("localhost", jobManagerPort);

			Configuration jmConfig = new Configuration();
			jmConfig.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_INTERVAL, "5 s");
			jmConfig.setString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE, "2000 s");
			jmConfig.setInteger(ConfigConstants.AKKA_WATCH_THRESHOLD, 10);
			jmConfig.setString(ConfigConstants.AKKA_ASK_TIMEOUT, "10 s");

			jmActorSystem = AkkaUtils.createActorSystem(jmConfig, new Some<Tuple2<String, Object>>(localAddress));
			ActorRef jmActor = JobManager.startJobManagerActors(
				jmConfig,
				jmActorSystem,
				JobManager.class,
				MemoryArchivist.class)._1();

			// the TaskManager java command
			String[] command = new String[] {
					javaCommand,
					"-Dlog.level=DEBUG",
					"-Dlog4j.configuration=file:" + tempLogFile.getAbsolutePath(),
					"-Xms80m", "-Xmx80m",
					"-classpath", getCurrentClasspath(),
					AbstractTaskManagerProcessFailureRecoveryTest.TaskManagerProcessEntryPoint.class.getName(),
					String.valueOf(jobManagerPort)
			};

			// start the first two TaskManager processes
			taskManagerProcess = new ProcessBuilder(command).start();
			new CommonTestUtils.PipeForwarder(taskManagerProcess.getErrorStream(), processOutput);
			
			// we wait for the JobManager to have the two TaskManagers available
			// since some of the CI environments are very hostile, we need to give this a lot of time (2 minutes)
			waitUntilNumTaskManagersAreRegistered(jmActor, 1, 120000);
			
			final Throwable[] errorRef = new Throwable[1];

			// start the test program, which infinitely blocks 
			Runnable programRunner = new Runnable() {
				@Override
				public void run() {
					try {
						ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", jobManagerPort);
						env.setParallelism(2);
						env.setNumberOfExecutionRetries(0);
						env.getConfig().disableSysoutLogging();

						env.generateSequence(0, Long.MAX_VALUE)

								.map(new MapFunction<Long, Long>() {

									@Override
									public Long map(Long value) throws Exception {
										synchronized (this) {
											wait();
										}
										return 0L;
									}
								})

								.output(new DiscardingOutputFormat<Long>());

						env.execute();
					}
					catch (Throwable t) {
						errorRef[0] = t;
					}
				}
			};
			
			Thread programThread = new Thread(programRunner);

			// kill the TaskManager
			taskManagerProcess.destroy();
			taskManagerProcess = null;

			// immediately submit the job. this should hit the case
			// where the JobManager still thinks it has the TaskManager and tries to send it tasks
			programThread.start();
			
			// try to cancel the job
			cancelRunningJob(jmActor);

			// we should see a failure within reasonable time (10s is the ask timeout).
			// since the CI environment is often slow, we conservatively give it up to 2 minutes, 
			// to fail, which is much lower than the failure time given by the heartbeats ( > 2000s)
			
			programThread.join(120000);
			
			assertFalse("The program did not cancel in time (2 minutes)", programThread.isAlive());
			
			Throwable error = errorRef[0];
			assertNotNull("The program did not fail properly", error);
			
			assertTrue(error instanceof ProgramInvocationException);
			// all seems well :-)
		}
		catch (Exception e) {
			e.printStackTrace();
			printProcessLog("TaskManager", processOutput.toString());
			fail(e.getMessage());
		}
		catch (Error e) {
			e.printStackTrace();
			printProcessLog("TaskManager 1", processOutput.toString());
			throw e;
		}
		finally {
			if (taskManagerProcess != null) {
				taskManagerProcess.destroy();
			}
			if (jmActorSystem != null) {
				jmActorSystem.shutdown();
			}
		}
	}
	
	private void cancelRunningJob(ActorRef jobManager) throws Exception {
		final FiniteDuration askTimeout = new FiniteDuration(10, TimeUnit.SECONDS);
		
		// try at most for 30 seconds
		final long deadline = System.currentTimeMillis() + 30000;

		JobID jobId = null;
		
		do {
			Future<Object> response = Patterns.ask(jobManager,
					JobManagerMessages.getRequestRunningJobsStatus(), new Timeout(askTimeout));

			Object result;
			try {
				result = Await.result(response, askTimeout);
			}
			catch (Exception e) {
				throw new Exception("Could not retrieve running jobs from the JobManager.", e);
			}

			if (result instanceof JobManagerMessages.RunningJobsStatus) {
	
				List<JobStatusMessage> jobs = ((JobManagerMessages.RunningJobsStatus) result).getStatusMessages();
				
				if (jobs.size() == 1) {
					jobId = jobs.get(0).getJobId();
					break;
				}
			}
		}
		while (System.currentTimeMillis() < deadline);

		if (jobId == null) {
			// we never found it running, must have failed already
			return;
		}
		
		// tell the JobManager to cancel the job
		jobManager.tell(new JobManagerMessages.CancelJob(jobId), ActorRef.noSender());
	}

	private void waitUntilNumTaskManagersAreRegistered(ActorRef jobManager, int numExpected, long maxDelay)
			throws Exception
	{
		final long deadline = System.currentTimeMillis() + maxDelay;
		while (true) {
			long remaining = deadline - System.currentTimeMillis();
			if (remaining <= 0) {
				fail("The TaskManagers did not register within the expected time (" + maxDelay + "msecs)");
			}

			FiniteDuration timeout = new FiniteDuration(remaining, TimeUnit.MILLISECONDS);

			try {
				Future<?> result = Patterns.ask(jobManager,
						JobManagerMessages.getRequestNumberRegisteredTaskManager(),
						new Timeout(timeout));
				Integer numTMs = (Integer) Await.result(result, timeout);
				if (numTMs == numExpected) {
					break;
				}
			}
			catch (TimeoutException e) {
				// ignore and retry
			}
			catch (ClassCastException e) {
				fail("Wrong response: " + e.getMessage());
			}
		}
	}

	private void printProcessLog(String processName, String log) {
		if (log == null || log.length() == 0) {
			return;
		}

		System.out.println("-----------------------------------------");
		System.out.println(" BEGIN SPAWNED PROCESS LOG FOR " + processName);
		System.out.println("-----------------------------------------");
		System.out.println(log);
		System.out.println("-----------------------------------------");
		System.out.println("		END SPAWNED PROCESS LOG");
		System.out.println("-----------------------------------------");
	}
}
