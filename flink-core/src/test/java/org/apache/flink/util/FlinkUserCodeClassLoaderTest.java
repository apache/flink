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

import java.net.URL;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link FlinkUserCodeClassLoader}.
 */
public class FlinkUserCodeClassLoaderTest extends TestLogger {
	@Test
	public void testExceptionHandling() {
		RuntimeException expectedException = new RuntimeException("Expected exception");
		AtomicReference<Throwable> handledException = new AtomicReference<>();
		try (FlinkUserCodeClassLoader classLoaderWithErrorHandler =
				new ThrowingURLClassLoader(handledException::set, expectedException)) {
			classLoaderWithErrorHandler.loadClass("dummy.class");
			fail("The expected exception is not thrown");
		} catch (Throwable t) {
			assertThat(handledException.get(), is(expectedException));
			assertThat(t, is(expectedException));
		}
	}

	private static class ThrowingURLClassLoader extends FlinkUserCodeClassLoader {
		private final RuntimeException expectedException;

		ThrowingURLClassLoader(Consumer<Throwable> classLoadingExceptionHandler, RuntimeException expectedException) {
			super(new URL[]{}, null, classLoadingExceptionHandler);
			this.expectedException = expectedException;
		}

		@Override
		protected Class<?> loadClassWithoutExceptionHandling(String name, boolean resolve) {
			throw expectedException;
		}
	}
}
