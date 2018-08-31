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

package org.apache.flink.configuration;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link CoreOptions}.
 */
public class CoreOptionsTest {
	@Test
	public void testGetParentFirstLoaderPatterns() {
		Configuration config = new Configuration();

		Assert.assertArrayEquals(
			CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS.defaultValue().split(";"),
			CoreOptions.getParentFirstLoaderPatterns(config));

		config.setString(CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS, "hello;world");

		Assert.assertArrayEquals(
			"hello;world".split(";"),
			CoreOptions.getParentFirstLoaderPatterns(config));

		config.setString(CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL, "how;are;you");

		Assert.assertArrayEquals(
			"hello;world;how;are;you".split(";"),
			CoreOptions.getParentFirstLoaderPatterns(config));

		config.setString(CoreOptions.ALWAYS_PARENT_FIRST_LOADER_PATTERNS, "");

		Assert.assertArrayEquals(
			"how;are;you".split(";"),
			CoreOptions.getParentFirstLoaderPatterns(config));
	}
}
