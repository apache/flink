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

package org.apache.flink.util;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the utility methods in {@link ExceptionUtils}.
 */
public class ExceptionUtilsTest extends TestLogger {

	@Test
	public void testStringifyNullException() {
		assertNotNull(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION);
		assertEquals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION, ExceptionUtils.stringifyException(null));
	}

	@Test
	public void testJvmFatalError() {
		// not all errors are fatal
		assertFalse(ExceptionUtils.isJvmFatalError(new Error()));

		// linkage errors are not fatal
		assertFalse(ExceptionUtils.isJvmFatalError(new LinkageError()));

		// some errors are fatal
		assertTrue(ExceptionUtils.isJvmFatalError(new InternalError()));
		assertTrue(ExceptionUtils.isJvmFatalError(new UnknownError()));
	}

	@Test
	public void testRethrowFatalError() {
		// fatal error is rethrown
		try {
			ExceptionUtils.rethrowIfFatalError(new InternalError());
			fail();
		} catch (InternalError ignored) {}

		// non-fatal error is not rethrown
		ExceptionUtils.rethrowIfFatalError(new NoClassDefFoundError());
	}

	@Test
	public void testFindThrowableByType() {
		assertTrue(ExceptionUtils.findThrowable(
			new RuntimeException(new IllegalStateException()),
			IllegalStateException.class).isPresent());
	}

	@Test
	public void testExceptionStripping() {
		final FlinkException expectedException = new FlinkException("test exception");
		final Throwable strippedException = ExceptionUtils.stripException(new RuntimeException(new RuntimeException(expectedException)), RuntimeException.class);

		assertThat(strippedException, is(equalTo(expectedException)));
	}

	@Test
	public void testInvalidExceptionStripping() {
		final FlinkException expectedException = new FlinkException(new RuntimeException(new FlinkException("inner exception")));
		final Throwable strippedException = ExceptionUtils.stripException(expectedException, RuntimeException.class);

		assertThat(strippedException, is(equalTo(expectedException)));
	}

	@Test
	public void testTryEnrichTaskExecutorErrorCanHandleNullValue() {
		assertThat(ExceptionUtils.tryEnrichOutOfMemoryError(null, "", ""), is(nullValue()));
	}

	@Test
	public void testIsMetaspaceOutOfMemoryErrorCanHandleNullValue() {
		assertFalse(ExceptionUtils.isMetaspaceOutOfMemoryError(null));
	}

	@Test
	public void testIsDirectOutOfMemoryErrorCanHandleNullValue() {
		assertFalse(ExceptionUtils.isDirectOutOfMemoryError(null));
	}
}
