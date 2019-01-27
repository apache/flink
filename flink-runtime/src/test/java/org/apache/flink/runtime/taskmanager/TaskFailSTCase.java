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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.stabilitytest.StabilityTestException;
import org.apache.flink.runtime.stabilitytest.ZookeeperClientServices;
import org.apache.flink.runtime.stabilitytest.ZookeeperClientServices.NodeRetrievalListener;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.flink.runtime.stabilitytest.Constants.LOG_PREFIX_FAULT_PROBABILITY;
import static org.apache.flink.runtime.stabilitytest.Constants.LOG_PREFIX_FAULT_TRIGGER;

/**
 * Stability test case class for task failure, it simulate to throw exceptions in task thread
 * via the given probability.
 *
 * <p>A stability test case class what be called ST-class must implements the static method
 * void initialize(Object arg0, Object arg1).
 *
 * <p>A stability test case what be called ST-case is one static method or the assemble of more
 * static methods in a ST-class. The first parameter of every ST-case method must be Boolean[],
 * it is a array contained one element and tell the caller whether step over all behind codes of
 * the injectable target method after invoke the ST-case method.
 */
public final class TaskFailSTCase {
	private static final Logger LOG = LoggerFactory.getLogger(TaskFailSTCase.class);

	/** The Random() instance for entry or exit fail. */
	private static final ThreadLocal<Random> entryExitRandom = new ThreadLocal<Random>();

	/** The Random() instance for running fail. */
	private static final ThreadLocal<Random> runningRandom = new ThreadLocal<Random>();

	/** The upper bound of random. */
	private static final int RANDOM_BOUND = Integer.MAX_VALUE;

	/** Probability of entry-exit-fail. */
	private static volatile double entryExitFailProb = 0.0;

	/** Probability of running-fail. */
	private static volatile double runningFailProb = 0.0;

	/** Zookeeper node for task's entry-exit-fail control. */
	public static final String ENTRY_ENXT_FAIL_ZKNODE_PATH = "/task_fail/entry_exit";

	/** Zookeeper node for task's running-fail control. */
	public static final String RUNNING_FAIL_ZKNODE_PATH = "/task_fail/running";

	/** The injection code line whether or not run in a task thread. */
	private static ThreadLocal<Boolean> isInTaskThread = new ThreadLocal<Boolean>();

	private static final String locationClassName = "org.jboss.byteman.rule.Rule";
	private static final String locationMethodName = "execute";
	private static final Map<String, Tuple2<Integer, Integer>> excludeMap = new HashMap<>();
	static {
		excludeMap.put("org.apache.flink.streaming.runtime.tasks.StreamTask", new Tuple2<Integer, Integer>(340, 394));
	}

	private static NodeRetrievalListener entryExitProbListener = new NodeRetrievalListener() {
		@Override
		public void notify(String data, Throwable throwable) {
			double oldFailProb = entryExitFailProb;

			if (data == null || throwable != null) {
				entryExitFailProb = 0.0;
			} else {
				try {
					double prob = Double.parseDouble(data);
					entryExitFailProb = Math.max(0.0, Math.min(1.0, prob));
				} catch (NumberFormatException nfe) {
					LOG.error("{} update task-entry-exit fail probability failed, ignore this value: {}."
						, LOG_PREFIX_FAULT_PROBABILITY, data);

					return;
				}
			}

			LOG.info("{} update task-entry-exit fail probability from {} to {}."
				, LOG_PREFIX_FAULT_PROBABILITY, oldFailProb, entryExitFailProb);
		}
	};

	private static NodeRetrievalListener runningProbListener = new NodeRetrievalListener() {
		@Override
		public void notify(String data, Throwable throwable) {
			double oldFailProb = runningFailProb;

			if (data == null || throwable != null) {
				runningFailProb = 0.0;
			} else {
				try {
					double prob = Double.parseDouble(data);
					runningFailProb = Math.max(0.0, Math.min(1.0, prob));
				} catch (NumberFormatException nfe) {
					LOG.error("{} update task-running fail probability failed, ignore this value: {}."
						, LOG_PREFIX_FAULT_PROBABILITY, data);

					return;
				}
			}

			LOG.info("{} update task-running fail probability from {} to {}."
				, LOG_PREFIX_FAULT_PROBABILITY, oldFailProb, runningFailProb);
		}
	};


	/**
	 * Connection to the ZooKeeper quorum, register entry-exit-fail and running-fail listeners.
	 * @param arg0 is a Properties object to initialize Zookeeper client settings
	 * @param arg1 is the prefix of property key which should be replaced
	 * @throws Exception
	 */
	public static void initialize(Object arg0, Object arg1) throws StabilityTestException {
		Preconditions.checkArgument(arg0 != null && arg0 instanceof Properties);
		Preconditions.checkArgument(arg1 == null || arg1 instanceof String);
		Properties properties = (Properties) arg0;
		String replacedPrefix = (String) arg1;

		try {
			if (!ZookeeperClientServices.isStarted()) {
				ZookeeperClientServices.start(new ZookeeperClientServices.Configuration(properties, replacedPrefix), true);
			}

			if (!ZookeeperClientServices.isListenerRegistered(entryExitProbListener)) {
				ZookeeperClientServices.registerListener(entryExitProbListener, ENTRY_ENXT_FAIL_ZKNODE_PATH, true);
			}

			if (!ZookeeperClientServices.isListenerRegistered(runningProbListener)) {
				ZookeeperClientServices.registerListener(runningProbListener, RUNNING_FAIL_ZKNODE_PATH, true);
			}
		} catch (Throwable t) {
			throw new StabilityTestException(t);
		}
	}

	/**
	 * Simulate task entry or exit fail via the given probability.
	 * @param isEarlyReturn tell the ST-Case helper whether return directly after invoke ST-Case method
	 * @param arg0 is the currently running object
	 * @param className the class name of arg0
	 * @param methodName the name of the currently running method
	 * @param location the currently running location
	 */
	public static void entryExitFail(Boolean[] isEarlyReturn, Object arg0, Object className, Object methodName, Object location) throws StabilityTestException {
		Preconditions.checkNotNull(arg0);
		Preconditions.checkNotNull(className);
		Preconditions.checkNotNull(methodName);
		Preconditions.checkNotNull(location);

		isEarlyReturn[0] = Boolean.FALSE; // Always do not intercept the target method

		if (entryExitFailProb <= 0 || !isInTaskThread(arg0)) {
			return;
		}

		// Let each thread have a random object
		Random random = entryExitRandom.get();
		if (random == null) {
			random = new Random();
			entryExitRandom.set(random);
		}

		int rnd = random.nextInt(RANDOM_BOUND);
		if (rnd <= RANDOM_BOUND * entryExitFailProb) {
			// Hit the probability and trigger fail on purpose
			StabilityTestException triggerException = new StabilityTestException(
					String.format("%s task-running[%s] fail, running method: %s.%s"
							, LOG_PREFIX_FAULT_TRIGGER, location, className, methodName)
			);

			LOG.info(LOG_PREFIX_FAULT_TRIGGER + " trigger task-fail by throwing the exception: ", triggerException);

			throw triggerException;
		}
	}


	/**
	 * Simulate task running fail via the given probability.
	 * @param isEarlyReturn tell the ST-Case helper whether return directly after invoke ST-Case method
	 * @param arg0 is the currently running object
	 * @param className the class name of arg0
	 * @param methodName the name of the currently running method
	 * @param line the currently running line
	 */
	public static void runningFail(Boolean[] isEarlyReturn, Object arg0, Object className, Object methodName, Object line) throws StabilityTestException {
		Preconditions.checkNotNull(arg0);
		Preconditions.checkNotNull(className);
		Preconditions.checkNotNull(methodName);
		Preconditions.checkArgument(line != null && line instanceof Integer);

		isEarlyReturn[0] = Boolean.FALSE; // Always do not intercept the target method

		if (runningFailProb <= 0 || !isInTaskThread(arg0)) {
			return;
		}

		// Let each thread have a random object
		Random random = runningRandom.get();
		if (random == null) {
			random = new Random();
			runningRandom.set(random);
		}

		int randomNum = random.nextInt(RANDOM_BOUND);
		if (randomNum <= RANDOM_BOUND * runningFailProb) {
			int excludeLine = excludeCurrentTestedLine(excludeMap, locationClassName, locationMethodName);
			if (excludeLine > 0) {
				LOG.info("{} skip task-running[AT LINE {}] failure, running method: {}.{}.", LOG_PREFIX_FAULT_TRIGGER, excludeLine, className, methodName);
				return;
			}

			// Hit the probability and trigger fail on purpose
			StabilityTestException triggerException = new StabilityTestException(
					String.format("%s task-running[AT LINE %d] fail, running method: %s.%s."
							, LOG_PREFIX_FAULT_TRIGGER, line, className, methodName)
			);

			LOG.info(LOG_PREFIX_FAULT_TRIGGER + " trigger task-fail by throwing the exception: ", triggerException);

			throw triggerException;
		}
	}

	private static boolean isInTaskThread(Object runObj) throws StabilityTestException {
		try {
			Thread thisThread = Thread.currentThread();

			// Use reflect to get the value of Thread's target member
			Field targetField = Thread.class.getDeclaredField("target");
			targetField.setAccessible(true);
			Object runnable = targetField.get(thisThread);

			return (runnable instanceof Task);
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new StabilityTestException(e);
		}

		/*
		Preconditions.checkNotNull(runObj);

		Boolean isTask = isInTaskThread.get();
		if (runObj instanceof Task) {
			if (isTask == null) {
				isInTaskThread.set(true);
			}
			return true;
		}

		return (isTask != null && isTask);
		*/
	}

	private static int excludeCurrentTestedLine(Map<String, Tuple2<Integer, Integer>> excludeMap,
											String locationClassName,
											String locationMethodName) {
		Tuple3<String, String, Integer> testedClassDetails = getTestedClassDetails(locationClassName, locationMethodName);

		if (testedClassDetails != null && excludeMap.get(testedClassDetails.f0) != null) {
			Tuple2<Integer, Integer> excludeLineRange = excludeMap.get(testedClassDetails.f0);
			Integer curLine = testedClassDetails.f2;

			if (curLine >= excludeLineRange.f0 && curLine <= excludeLineRange.f1) {
				return curLine;
			}
		}

		return -1;
	}

	private static Tuple3<String, String, Integer> getTestedClassDetails(String locationClassName, String locationMethodName) {
		boolean isFound = false;

		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		for (StackTraceElement element : elements) {
			if (locationClassName.equals(element.getClassName())
				&& locationMethodName.equals(element.getMethodName())) {
				isFound = true;
			} else if (isFound) {
				final Tuple3<String, String, Integer> result = new Tuple3<>();

				result.f0 = element.getClassName();
				result.f1 = element.getMethodName();
				result.f2 = element.getLineNumber();

				return result;
			}
		}

		return null;
	}

	public static void main(String[] args) {
		String thisClassName = TaskFailSTCase.class.getName();

		Map<String, Tuple2<Integer, Integer>> excludeMap = Collections.singletonMap(thisClassName, new Tuple2<>(330, 331));

		Assert.assertTrue(excludeCurrentTestedLine(excludeMap, thisClassName, "excludeCurrentTestedLine") > 0);
		Assert.assertTrue(excludeCurrentTestedLine(excludeMap, thisClassName, "excludeCurrentTestedLine") > 0);
		Assert.assertTrue(excludeCurrentTestedLine(excludeMap, thisClassName, "excludeCurrentTestedLine") == -1);
	}
}
