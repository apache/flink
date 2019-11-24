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

import org.junit.Test;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests to guard {@link RocksDBOptionsContainer}.
 */
public class RocksDBOptionsContainerTest {

	@Test
	public void testCloseOptionsFactory() throws Exception {
		RocksDBOptionsContainer container = new RocksDBOptionsContainer();
		DefaultConfigurableOptionsFactory optionsFactory = new DefaultConfigurableOptionsFactory();
		container.setOptionsFactory(optionsFactory);
		assertThat(optionsFactory.isClosed(), is(false));
		container.close();
		assertThat(optionsFactory.isClosed(), is(true));
	}

	@Test
	public void testFreeDBOptionsAfterClose() throws Exception {
		RocksDBOptionsContainer container = new RocksDBOptionsContainer();
		DBOptions dbOptions = container.getDbOptions();
		assertThat(dbOptions.isOwningHandle(), is(true));
		container.close();
		assertThat(dbOptions.isOwningHandle(), is(false));
	}

	@Test
	public void testFreeMultipleDBOptionsAfterClose() throws Exception {
		RocksDBOptionsContainer container = new RocksDBOptionsContainer();
		final int optionNumber = 20;
		ArrayList<DBOptions> dbOptions = new ArrayList<>(optionNumber);
		for (int i = 0; i < optionNumber; i++) {
			dbOptions.add(container.getDbOptions());
		}
		container.close();
		for (DBOptions dbOption: dbOptions) {
			assertThat(dbOption.isOwningHandle(), is(false));
		}
	}

	@Test
	public void testFreeColumnOptionsAfterClose() throws Exception {
		RocksDBOptionsContainer container = new RocksDBOptionsContainer();
		ColumnFamilyOptions columnFamilyOptions = container.getColumnOptions();
		assertThat(columnFamilyOptions.isOwningHandle(), is(true));
		container.close();
		assertThat(columnFamilyOptions.isOwningHandle(), is(false));
	}

	@Test
	public void testFreeMultipleColumnOptionsAfterClose() throws Exception {
		RocksDBOptionsContainer container = new RocksDBOptionsContainer();
		final int optionNumber = 20;
		ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
		for (int i = 0; i < optionNumber; i++) {
			columnFamilyOptions.add(container.getColumnOptions());
		}
		container.close();
		for (ColumnFamilyOptions columnFamilyOption: columnFamilyOptions) {
			assertThat(columnFamilyOption.isOwningHandle(), is(false));
		}
	}

	@Test
	public void testFreeMultipleColumnOptionsWithPredefinedOptions() throws Exception {
		RocksDBOptionsContainer container = new RocksDBOptionsContainer();
		for (PredefinedOptions predefinedOptions: PredefinedOptions.values()) {
			container.setPredefinedOptions(predefinedOptions);
			final int optionNumber = 20;
			ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
			for (int i = 0; i < optionNumber; i++) {
				columnFamilyOptions.add(container.getColumnOptions());
			}
			container.close();
			for (ColumnFamilyOptions columnFamilyOption: columnFamilyOptions) {
				assertThat(columnFamilyOption.isOwningHandle(), is(false));
			}
		}
	}
}
