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

package org.apache.flink.util;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link WrappingProxyUtil}.
 */
public class WrappingProxyUtilTest {

	@Test
	public void testThrowsExceptionIfTooManyProxies() {
		try {
			WrappingProxyUtil.stripProxy(new SelfWrappingProxy(WrappingProxyUtil.SAFETY_NET_MAX_ITERATIONS));
			fail("Expected exception not thrown");
		} catch (final IllegalArgumentException e) {
			assertThat(e.getMessage(), containsString("Are there loops in the object graph?"));
		}
	}

	@Test
	public void testStripsAllProxies() {
		final SelfWrappingProxy wrappingProxy = new SelfWrappingProxy(WrappingProxyUtil.SAFETY_NET_MAX_ITERATIONS - 1);
		assertThat(WrappingProxyUtil.stripProxy(wrappingProxy), is(not(instanceOf(SelfWrappingProxy.class))));
	}

	private static class Wrapped {
	}

	/**
	 * Wraps around {@link Wrapped} a specified number of times.
	 */
	private static class SelfWrappingProxy extends Wrapped implements WrappingProxy<Wrapped> {

		private int levels;

		private SelfWrappingProxy(final int levels) {
			this.levels = levels;
		}

		@Override
		public Wrapped getWrappedDelegate() {
			if (levels-- == 0) {
				return new Wrapped();
			} else {
				return this;
			}
		}
	}

}
