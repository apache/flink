/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.plugin;

import org.apache.flink.util.ChildFirstClassLoader;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

/**
 * Test for {@link TemporaryClassLoaderContext}.
 */
public class TemporaryClassLoaderContextTest {

	@Test
	public void testTemporaryClassLoaderContext() {
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

		final ChildFirstClassLoader temporaryClassLoader =
			new ChildFirstClassLoader(new URL[0], contextClassLoader, new String[0]);

		try (TemporaryClassLoaderContext classLoaderContext = new TemporaryClassLoaderContext(temporaryClassLoader)) {
			Assert.assertEquals(temporaryClassLoader, Thread.currentThread().getContextClassLoader());
		}

		Assert.assertEquals(contextClassLoader, Thread.currentThread().getContextClassLoader());
	}
}
