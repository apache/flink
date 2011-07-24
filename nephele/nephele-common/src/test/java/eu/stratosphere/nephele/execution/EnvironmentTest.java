/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.execution;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.util.CommonTestUtils;

/**
 * This class include tests concerning the {@link Environment} class and its auxiliary structures.
 * 
 * @author warneke
 */
public class EnvironmentTest {

	/**
	 * The name of the task used during the tests.
	 */
	private static final String TASK_NAME = "Test Task 1";

	/**
	 * The job ID to be used during the tests.
	 */
	private static final JobID JOB_ID = new JobID();

	/**
	 * This enumeration defines three possible execution scenarios.
	 * 
	 * @author warneke
	 */
	private static enum ExecutionScenario {
		REGULAR, EXCEPTION, USER_ABORT
	};

	/**
	 * The maximum time interval to wait for task completion in milliseconds.
	 */
	private static long MAX_WAIT_TIME = 1000;

	/**
	 * A test {@link ExecutionListener} implementation.
	 * 
	 * @author warneke
	 */
	private static class TestExecutionListener implements ExecutionListener {

		/**
		 * The expected environment object.
		 */
		private final Environment expectedEnvironment;

		/**
		 * The execution scneario.
		 */
		private volatile AssertionError assertionError = null;

		/**
		 * The sequence of expected state changes in this scenario.
		 */
		private final ExecutionState[] expectedStateChanges;

		/**
		 * Pointer to the current execution state;
		 */
		private volatile int executionStateIndex = 0;

		/**
		 * <code>true</code> if the task execution has been started already, <code>false</code> otherwise.
		 */
		private volatile boolean executionStarted = false;

		/**
		 * Constructs a new test execution listener.
		 * 
		 * @param environment
		 *        the environment object which is expected to be passed for execution state changes
		 * @param scenario
		 *        the execution scenario which defines the expected sequence of state changes
		 */
		public TestExecutionListener(Environment environment, ExecutionScenario scenario) {
			this.expectedEnvironment = environment;

			switch (scenario) {
			case REGULAR:
				this.expectedStateChanges = new ExecutionState[] { ExecutionState.RUNNING, ExecutionState.FINISHING,
					ExecutionState.FINISHED };
				break;
			case EXCEPTION:
				this.expectedStateChanges = new ExecutionState[] { ExecutionState.RUNNING, ExecutionState.FAILED };
				break;
			default: // Must be USER_ABORT
				this.expectedStateChanges = new ExecutionState[] { ExecutionState.RUNNING, ExecutionState.CANCELING,
					ExecutionState.CANCELED };
				break;
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void executionStateChanged(Environment ee, ExecutionState newExecutionState, String optionalMessage) {

			if (this.assertionError != null) {
				return;
			}

			if (newExecutionState == ExecutionState.RUNNING) {
				this.executionStarted = true;
			}

			try {
				assertEquals(this.expectedEnvironment, ee);
				assertTrue(this.executionStateIndex < this.expectedStateChanges.length);
				assertEquals(this.expectedStateChanges[this.executionStateIndex++], newExecutionState);
			} catch (AssertionError e) {
				this.assertionError = e;
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void userThreadStarted(Environment ee, Thread userThread) {
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void userThreadFinished(Environment ee, Thread userThread) {
		}

		/**
		 * Waits either for the current execution scenario to complete. If <code>MAX_WAIT_TIME</code> is elapsed before
		 * the scenario is completed the test is aborted as failed.
		 */
		public void waitForExecutionScenarioToComplete() {

			final long startTime = System.currentTimeMillis();

			while (true) {

				if (this.assertionError != null) {
					throw this.assertionError;
				}

				if (this.executionStateIndex == this.expectedStateChanges.length) {
					return;
				}

				if ((startTime + MAX_WAIT_TIME) < System.currentTimeMillis()) {
					fail("Scenario took to long to complete");
				}

				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					break;
				}
			}
		}

		/**
		 * Waits until the task execution started and the task thread set the execution state to RUNNING. If
		 * <code>MAX_WAIT_TIME</code> is elapsed before the scenario is completed the test is aborted as failed.
		 */
		public void waitUntilTaskExecutionStarted() {

			final long startTime = System.currentTimeMillis();

			while (!this.executionStarted) {

				if (this.assertionError != null) {
					throw this.assertionError;
				}

				if ((startTime + MAX_WAIT_TIME) < System.currentTimeMillis()) {
					fail("Scenario took to long to complete");
				}

				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
	}

	/**
	 * Registers the job ID which is used during the tests with the library cache manager.
	 */
	@BeforeClass
	public static void registerRequiredJarFiles() {

		try {
			LibraryCacheManager.register(JOB_ID, new String[0]);
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * Unregisters the job ID which is used during the tests with the library cache manager.
	 */
	@AfterClass
	public static void unregisterRequiredJarFiles() {

		try {
			LibraryCacheManager.unregister(JOB_ID);
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * This test checks the correct serialization/deserialization of an {@link Environment} object.
	 */
	@Test
	public void testSerialization() {

		try {
			final Environment orig = constructTestEnvironment();
			try {
				orig.instantiateInvokable();
			} catch (Exception e) {
				fail(e.getMessage());
			}

			final Environment copy = (Environment) CommonTestUtils.createCopy(orig);

			assertEquals(orig.getJobID(), copy.getJobID());
			assertEquals(orig.getExecutionState(), copy.getExecutionState());
			assertEquals(orig.getCurrentNumberOfSubtasks(), copy.getCurrentNumberOfSubtasks());
			assertEquals(orig.getIndexInSubtaskGroup(), copy.getIndexInSubtaskGroup());

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * This test checks the error handling when no invokable class is set.
	 */
	@Test
	public void testSerializationWithInvalidInvokable() {

		final Environment orig = new Environment(JOB_ID, TASK_NAME, null, new Configuration());
		try {
			CommonTestUtils.createCopy(orig); // This is expected to throw an IOException
		} catch (IOException ioe) {
			return;
		}

		fail("createCopy did not throw an IOException although in invokable class is given");
	}

	/**
	 * This test checks the instantiation of a Nephele task inside an environment.
	 */
	@Test
	public void testTaskInstantiation() {

		final Environment env = constructTestEnvironment();

		assertNull(env.getInvokable());
		assertEquals(0, env.getNumberOfInputGates());
		assertEquals(0, env.getNumberOfOutputGates());

		try {
			env.instantiateInvokable();
		} catch (Exception e) {
			fail(e.getMessage());
		}

		assertNotNull(env.getInvokable());
		assertEquals(SampleTask.NUMBER_OF_RECORD_READER, env.getNumberOfInputGates());
		assertEquals(SampleTask.NUMBER_OF_RECORD_WRITER, env.getNumberOfOutputGates());
	}

	/**
	 * This tests checks the life cycle of a task in case no error or user-requested abort occurs.
	 */
	@Test
	public void testRegularTaskExecution() {

		final Environment env = constructTestEnvironment();

		try {
			env.instantiateInvokable();
		} catch (Exception e) {
			fail(e.getMessage());
		}

		final TestExecutionListener executionListener = new TestExecutionListener(env, ExecutionScenario.REGULAR);
		env.registerExecutionListener(executionListener);
		env.startExecution();
		executionListener.waitForExecutionScenarioToComplete();
		env.unregisterExecutionListener(executionListener);

		checkInputOutputGates(env);
	}

	/**
	 * This test checks the life cycle of a task in case an exception occurs during its execution.
	 */
	@Test
	public void testTaskExecutionWithException() {

		final Environment env = constructTestEnvironment();
		env.getRuntimeConfiguration().setBoolean(SampleTask.EXCEPTION_SCENARIO_KEY, true);

		try {
			env.instantiateInvokable();
		} catch (Exception e) {
			fail(e.getMessage());
		}

		final TestExecutionListener executionListener = new TestExecutionListener(env, ExecutionScenario.EXCEPTION);
		env.registerExecutionListener(executionListener);
		env.startExecution();
		executionListener.waitForExecutionScenarioToComplete();
		env.unregisterExecutionListener(executionListener);

		checkInputOutputGates(env);
	}

	/**
	 * This test checks the life cycle of a task in case the user aborts the execution.
	 */
	@Test
	public void testTaskExecutionWithUserAbort() {

		final Environment env = constructTestEnvironment();
		env.getRuntimeConfiguration().setBoolean(SampleTask.USER_ABORT_SCENARIO_KEY, true);

		try {
			env.instantiateInvokable();
		} catch (Exception e) {
			fail(e.getMessage());
		}

		final TestExecutionListener executionListener = new TestExecutionListener(env, ExecutionScenario.USER_ABORT);
		env.registerExecutionListener(executionListener);
		env.startExecution();

		// Wait until task execution started, then cancel the execution
		executionListener.waitUntilTaskExecutionStarted();
		env.cancelExecution();

		executionListener.waitForExecutionScenarioToComplete();
		env.unregisterExecutionListener(executionListener);

		checkInputOutputGates(env);
	}

	/**
	 * Checks if all input and output gates attached to the given environment are properly closed.
	 * 
	 * @param environment
	 *        the environment whose input/output shall be checked
	 */
	private void checkInputOutputGates(final Environment environment) {

		try {
			for (int i = 0; i < environment.getNumberOfInputGates(); i++) {
				assertTrue(environment.getInputGate(i).isClosed());
			}

			for (int i = 0; i < environment.getNumberOfOutputGates(); i++) {
				assertTrue(environment.getOutputGate(i).isClosed());
			}
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} catch (InterruptedException ie) {
			fail(ie.getMessage());
		}
	}

	/**
	 * Constructs the environment which is used during the tests.
	 * 
	 * @return the environment used during the tests
	 */
	private Environment constructTestEnvironment() {

		final Configuration runtimeConfig = new Configuration();
		final Environment env = new Environment(JOB_ID, TASK_NAME, SampleTask.class, runtimeConfig);

		env.setCurrentNumberOfSubtasks(2);
		env.setIndexInSubtaskGroup(0);

		return env;
	}
}
