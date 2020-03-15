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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.Configuration;

import org.junit.Test;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests that the changes introducing the {@link RocksDBOptionsFactory} are backwards compatible.
 */
public class RocksDBOptionsFactoryCompatibilityTest {

	@Test
	public void testInheritance() {
		assertThat(new DefaultConfigurableOptionsFactory(), instanceOf(RocksDBOptionsFactory.class));
	}

	@Test
	public void testSetAndGet() throws Exception {
		final RocksDBStateBackend backend = new RocksDBStateBackend("file:///a/b/c");
		final OptionsFactory testFactory = new TestOptionsFactory();

		backend.setOptions(testFactory);

		assertSame(testFactory, backend.getOptions());
	}

	@Test
	public void testConfiguration() throws Exception {
		final RocksDBStateBackend backend = new RocksDBStateBackend("file:///a/b/c");
		final OptionsFactory testFactory = new TestOptionsFactory();

		backend.setOptions(testFactory);

		final TestOptionsFactory reconfigured = (TestOptionsFactory) backend
				.configure(new Configuration(), getClass().getClassLoader())
				.getOptions();

		assertTrue(reconfigured.wasConfigured);
	}

	// ------------------------------------------------------------------------

	private static class TestOptionsFactory implements ConfigurableOptionsFactory {

		boolean wasConfigured;

		@Override
		public OptionsFactory configure(Configuration configuration) {
			wasConfigured = true;
			return this;
		}

		@Override
		public DBOptions createDBOptions(DBOptions currentOptions) {
			return currentOptions;
		}

		@Override
		public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
			return currentOptions;
		}
	}
}
