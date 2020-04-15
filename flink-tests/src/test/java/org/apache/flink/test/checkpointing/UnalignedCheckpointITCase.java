/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runners.model.Statement;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertThat;

/**
 * Integration test for performing the unaligned checkpoint.
 */
public class UnalignedCheckpointITCase extends TestLogger {
	public static final String NUM_COMPLETED_CHECKPOINTS = "numCompletedCheckpoints";

	@Rule
	public final TemporaryFolder temp = new TemporaryFolder();

	@Rule
	public final Timeout timeout = new Timeout(90, TimeUnit.SECONDS) {
		@Override
		protected Statement createFailOnTimeoutStatement(Statement statement) throws Exception {

			class CallableStatement implements Callable<Throwable> {
				private final CountDownLatch startLatch = new CountDownLatch(1);

				public Throwable call() throws Exception {
					try {
						startLatch.countDown();
						statement.evaluate();
					} catch (Exception e) {
						throw e;
					} catch (Throwable e) {
						return e;
					}
					return null;
				}

				public void awaitStarted() throws InterruptedException {
					startLatch.await();
				}
			}

			return new Statement() {
				@Override
				public void evaluate() throws Throwable {
					CallableStatement callable = new CallableStatement();
					FutureTask<Throwable> task = new FutureTask<Throwable>(callable);
					final ThreadGroup threadGroup = new ThreadGroup("FailOnTimeoutGroup");
					Thread thread = new Thread(threadGroup, task, "Time-limited test");
					thread.setDaemon(true);
					thread.start();
					callable.awaitStarted();
					Throwable throwable = getResult(task);
					if (throwable != null) {
						throw throwable;
					}
				}

				/**
				 * Wait for the test task, returning the exception thrown by the test if the test
				 * failed, an exception indicating a timeout if the test timed out, or {@code null}
				 * if the test passed.
				 */
				private Throwable getResult(FutureTask<Throwable> task) {
					try {
						if (getTimeout(TimeUnit.MILLISECONDS) > 0) {
							return task.get(getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
						} else {
							return task.get();
						}
					} catch (InterruptedException e) {
						return e; // caller will re-throw; no need to call Thread.interrupt()
					} catch (ExecutionException e) {
						// test failed; have caller re-throw the exception thrown by the test
						return e.getCause();
					} catch (TimeoutException e) {
						return new AssertionError(buildThreadDiagnosticString());
					}
				}
			};
		}
	};

	@Test
	public void shouldPerformUnalignedCheckpointOnNonparallelTopology() throws Exception {
		execute(1);
	}

	@Test
	public void shouldPerformUnalignedCheckpointOnLocalChannelsOnly() throws Exception {
		execute(2);
	}

	@Test
	public void shouldPerformUnalignedCheckpointOnRemoteChannels() throws Exception {
		execute(10);
	}

	@Test
	public void shouldPerformUnalignedCheckpointMassivelyParallel() throws Exception {
		execute(20);
	}

	private void execute(int paralellism) throws Exception {
		StreamExecutionEnvironment env = createEnv(paralellism);

		createDAG(env, 30);
		final JobExecutionResult executionResult = env.execute();

		assertThat(executionResult.<Long>getAccumulatorResult(NUM_COMPLETED_CHECKPOINTS) / paralellism,
			Matchers.greaterThanOrEqualTo(30L));
	}

	@Nonnull
	private LocalStreamEnvironment createEnv(final int parallelism) throws IOException {
		Configuration conf = new Configuration();
		final int numSlots = 5;
		conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlots);
		conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, .9f);
		conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, (parallelism + numSlots - 1) / numSlots);

		conf.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temp.newFolder().toURI().toString());

		final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);
		env.enableCheckpointing(100);
		env.getCheckpointConfig().enableUnalignedCheckpoints();
		return env;
	}

	private void createDAG(final StreamExecutionEnvironment env, final long minCheckpoints) {
		final SingleOutputStreamOperator<Integer> source = env.addSource(new IntegerSource(minCheckpoints));
		final SingleOutputStreamOperator<Integer> transform = source.shuffle().map(i -> 2 * i);
		transform.shuffle().addSink(new CountingSink<>());
	}

	private final static String INDENT = "    ";

	public static String buildThreadDiagnosticString() {
		StringWriter sw = new StringWriter();
		PrintWriter output = new PrintWriter(sw);

		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
		output.println(String.format("Timestamp: %s", dateFormat.format(new Date())));
		output.println();
		output.println(buildThreadDump());

		String deadlocksInfo = buildDeadlockInfo();
		if (deadlocksInfo != null) {
			output.println("====> DEADLOCKS DETECTED <====");
			output.println();
			output.println(deadlocksInfo);
		}

		return sw.toString();
	}

	static String buildThreadDump() {
		StringBuilder dump = new StringBuilder();
		Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
		for (Map.Entry<Thread, StackTraceElement[]> e : stackTraces.entrySet()) {
			Thread thread = e.getKey();
			dump.append(String.format(
				"\"%s\" %s prio=%d tid=%d %s\njava.lang.Thread.State: %s",
				thread.getName(),
				thread.isDaemon() ? "daemon" : "",
				thread.getPriority(),
				thread.getId(),
				Thread.State.WAITING.equals(thread.getState()) ?
					"in Object.wait()" :
					thread.getState().name().toLowerCase(),
				Thread.State.WAITING.equals(thread.getState()) ?
					"WAITING (on object monitor)" : thread.getState()));
			for (StackTraceElement stackTraceElement : e.getValue()) {
				dump.append("\n        at ");
				dump.append(stackTraceElement);
			}
			dump.append("\n");
		}
		return dump.toString();
	}

	static String buildDeadlockInfo() {
		ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
		long[] threadIds = threadBean.findMonitorDeadlockedThreads();
		if (threadIds != null && threadIds.length > 0) {
			StringWriter stringWriter = new StringWriter();
			PrintWriter out = new PrintWriter(stringWriter);

			ThreadInfo[] infos = threadBean.getThreadInfo(threadIds, true, true);
			for (ThreadInfo ti : infos) {
				printThreadInfo(ti, out);
				printLockInfo(ti.getLockedSynchronizers(), out);
				out.println();
			}

			out.close();
			return stringWriter.toString();
		} else {
			return null;
		}
	}

	private static void printThreadInfo(ThreadInfo ti, PrintWriter out) {
		// print thread information
		printThread(ti, out);

		// print stack trace with locks
		StackTraceElement[] stacktrace = ti.getStackTrace();
		MonitorInfo[] monitors = ti.getLockedMonitors();
		for (int i = 0; i < stacktrace.length; i++) {
			StackTraceElement ste = stacktrace[i];
			out.println(INDENT + "at " + ste.toString());
			for (MonitorInfo mi : monitors) {
				if (mi.getLockedStackDepth() == i) {
					out.println(INDENT + "  - locked " + mi);
				}
			}
		}
		out.println();
	}

	private static void printThread(ThreadInfo ti, PrintWriter out) {
		out.print("\"" + ti.getThreadName() + "\"" + " Id="
			+ ti.getThreadId() + " in " + ti.getThreadState());
		if (ti.getLockName() != null) {
			out.print(" on lock=" + ti.getLockName());
		}
		if (ti.isSuspended()) {
			out.print(" (suspended)");
		}
		if (ti.isInNative()) {
			out.print(" (running in native)");
		}
		out.println();
		if (ti.getLockOwnerName() != null) {
			out.println(INDENT + " owned by " + ti.getLockOwnerName() + " Id="
				+ ti.getLockOwnerId());
		}
	}

	private static void printLockInfo(LockInfo[] locks, PrintWriter out) {
		out.println(INDENT + "Locked synchronizers: count = " + locks.length);
		for (LockInfo li : locks) {
			out.println(INDENT + "  - " + li);
		}
		out.println();
	}

	private static class IntegerSource extends RichParallelSourceFunction<Integer> implements CheckpointListener {

		private final long minCheckpoints;
		private volatile boolean running = true;
		private LongCounter numCompletedCheckpoints = new LongCounter();

		public IntegerSource(final long minCheckpoints) {
			this.minCheckpoints = minCheckpoints;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator(NUM_COMPLETED_CHECKPOINTS, numCompletedCheckpoints);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			numCompletedCheckpoints.add(1);
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			int counter = 0;
			while (running) {
				ctx.collect(counter++);

				if (numCompletedCheckpoints.getLocalValue() >= minCheckpoints) {
					cancel();
				}
			}

			// wait for all instances to finish, such that checkpoints are still processed
			Thread.sleep(1000);
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class CountingSink<T> extends RichSinkFunction<T> {
		private LongCounter counter = new LongCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator("outputs", counter);
		}

		@Override
		public void invoke(T value, Context context) throws Exception {
			counter.add(1);
			if (counter.getLocalValue() % 100 == 0) {
				Thread.sleep(1);
			}
		}
	}
}
