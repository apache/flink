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
import java.net.URLClassLoader;

import static org.junit.Assert.assertSame;

/**
 * Tests for the {@link LambdaUtil}.
 */
public class LambdaUtilTest {

	@Test
	public void testRunWithContextClassLoaderRunnable() throws Exception {
		final ClassLoader aPrioriContextClassLoader = Thread.currentThread().getContextClassLoader();

		try {
			final ClassLoader original = new URLClassLoader(new URL[0]);
			final ClassLoader temp = new URLClassLoader(new URL[0]);

			// set the original context class loader
			Thread.currentThread().setContextClassLoader(original);

			LambdaUtil.withContextClassLoader(temp, () ->
					assertSame(temp, Thread.currentThread().getContextClassLoader()));

			// make sure the method restored the the original context class loader
			assertSame(original, Thread.currentThread().getContextClassLoader());
		}
		finally {
			Thread.currentThread().setContextClassLoader(aPrioriContextClassLoader);
		}
	}

	@Test
	public void testRunWithContextClassLoaderSupplier() throws Exception {
		final ClassLoader aPrioriContextClassLoader = Thread.currentThread().getContextClassLoader();

		try {
			final ClassLoader original = new URLClassLoader(new URL[0]);
			final ClassLoader temp = new URLClassLoader(new URL[0]);

			// set the original context class loader
			Thread.currentThread().setContextClassLoader(original);

			LambdaUtil.withContextClassLoader(temp, () -> {
				assertSame(temp, Thread.currentThread().getContextClassLoader());
				return true;
			});

			// make sure the method restored the the original context class loader
			assertSame(original, Thread.currentThread().getContextClassLoader());
		}
		finally {
			Thread.currentThread().setContextClassLoader(aPrioriContextClassLoader);
		}
	}
}
