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

package org.apache.flink.test.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

/**
 * Test utilities.
 */
public class TestUtils {

	public static JobExecutionResult tryExecute(StreamExecutionEnvironment see, String name) throws Exception {
		try {
			return see.execute(name);
		}
		catch (ProgramInvocationException | JobExecutionException root) {
			Throwable cause = root.getCause();

			// search for nested SuccessExceptions
			int depth = 0;
			while (!(cause instanceof SuccessException)) {
				if (cause == null || depth++ == 20) {
					root.printStackTrace();
					fail("Test failed: " + root.getMessage());
				}
				else {
					cause = cause.getCause();
				}
			}
		}

		return null;
	}

	/**
	 * Wait indefinitely until the given condition is met.
	 *
	 * @param condition the condition to wait for.
	 * @throws Exception
	 */
	public static void waitUntil(ThrowingSupplier<Boolean> condition) throws Exception {
		waitUntil("No Error Message", condition, Long.MAX_VALUE);
	}

	/**
	 * Wait until the given condition is met or timeout, whichever happens first.
	 *
	 * @param errMsg the error message if the condition is not met.
	 * @param condition the condition to wait for.
	 * @param timeoutMs the maximum time to wait in milliseconds.
	 * @throws Exception
	 */
	public static void waitUntil(String errMsg, ThrowingSupplier<Boolean> condition, long timeoutMs) throws Exception {
		long now = System.currentTimeMillis();
		long deadline = timeoutMs == Long.MAX_VALUE ? Long.MAX_VALUE : now + timeoutMs;

		while (now <= deadline) {
			if (condition.apply()) {
				return;
			}
			Thread.sleep(1);
			now = System.currentTimeMillis();
		}
		throw new TimeoutException(errMsg);
	}

	/**
	 * A supplier lambda function interface that allows exception to be thrown.
	 * @param <T>
	 */
	@FunctionalInterface
	public interface ThrowingSupplier<T> {
		T apply() throws Exception;
	}
}
