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

package org.apache.flink.testutils.junit;

import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rule to retry failed tests for a fixed number of times.
 *
 * <p>Add the {@link RetryRule} to your test and annotate tests with {@link RetryOnFailure}.
 *
 * <pre>
 * public class YourTest {
 *
 *     {@literal @}Rule
 *     public RetryRule retryRule = new RetryRule();
 *
 *     {@literal @}Test
 *     {@literal @}RetryOnFailure(times=1)
 *     public void yourTest() {
 *         // This will be retried 1 time (total runs 2) before failing the test.
 *         throw new Exception("Failing test");
 *     }
 * }
 * </pre>
 */
public class RetryRule implements TestRule {

	public static final Logger LOG = LoggerFactory.getLogger(RetryRule.class);

	@Override
	public Statement apply(Statement statement, Description description) {
		RetryOnFailure retryOnFailure = description.getAnnotation(RetryOnFailure.class);
		RetryOnException retryOnException = description.getAnnotation(RetryOnException.class);

		// sanity check that we don't use expected exceptions with the RetryOnX annotations
		if (retryOnFailure != null || retryOnException != null) {
			Test test = description.getAnnotation(Test.class);
			if (test.expected() != Test.None.class) {
				throw new IllegalArgumentException("You cannot combine the RetryOnFailure " +
						"annotation with the Test(expected) annotation.");
			}
		}

		// sanity check that we don't use both annotations
		if (retryOnFailure != null && retryOnException != null) {
			throw new IllegalArgumentException(
					"You cannot combine the RetryOnFailure and RetryOnException annotations.");
		}

		if (retryOnFailure != null) {
			return new RetryOnFailureStatement(retryOnFailure.times(), statement);
		}
		else if (retryOnException != null) {
			return new RetryOnExceptionStatement(retryOnException.times(), retryOnException.exception(), statement);
		}
		else {
			return statement;
		}
	}

	/**
	 * Retries a test in case of a failure.
	 */
	private static class RetryOnFailureStatement extends Statement {

		private final int timesOnFailure;

		private int currentRun;

		private final Statement statement;

		private RetryOnFailureStatement(int timesOnFailure, Statement statement) {
			if (timesOnFailure < 0) {
				throw new IllegalArgumentException("Negatives number of retries on failure");
			}
			this.timesOnFailure = timesOnFailure;
			this.statement = statement;
		}

		/**
		 * Retry a test in case of a failure.
		 *
		 * @throws Throwable
		 */
		@Override
		public void evaluate() throws Throwable {
			for (currentRun = 0; currentRun <= timesOnFailure; currentRun++) {
				try {
					statement.evaluate();
					break; // success
				}
				catch (Throwable t) {
					LOG.warn(String.format("Test run failed (%d/%d).",
							currentRun, timesOnFailure + 1), t);

					// Throw the failure if retried too often
					if (currentRun == timesOnFailure) {
						throw t;
					}
				}
			}
		}
	}

	/**
	 * Retries a test in case of a failure.
	 */
	private static class RetryOnExceptionStatement extends Statement {

		private final Class<? extends Throwable> exceptionClass;
		private final int timesOnFailure;
		private final Statement statement;

		private int currentRun;

		private RetryOnExceptionStatement(int timesOnFailure, Class<? extends Throwable> exceptionClass, Statement statement) {
			if (timesOnFailure < 0) {
				throw new IllegalArgumentException("Negatives number of retries on failure");
			}
			if (exceptionClass == null) {
				throw new NullPointerException("exceptionClass");
			}

			this.exceptionClass = (exceptionClass);
			this.timesOnFailure = timesOnFailure;
			this.statement = statement;
		}

		/**
		 * Retry a test in case of a failure with a specific exception.
		 *
		 * @throws Throwable
		 */
		@Override
		public void evaluate() throws Throwable {
			for (currentRun = 0; currentRun <= timesOnFailure; currentRun++) {
				try {
					statement.evaluate();
					break; // success
				}
				catch (Throwable t) {
					LOG.warn(String.format("Test run failed (%d/%d).", currentRun, timesOnFailure + 1), t);

					if (!exceptionClass.isAssignableFrom(t.getClass()) || currentRun >= timesOnFailure) {
						// Throw the failure if retried too often, or if it is the wrong exception
						throw t;
					}
				}
			}
		}
	}
}
