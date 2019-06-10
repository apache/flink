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

package org.apache.flink.table.catalog.hive.factories;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogDescriptor;
import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link HiveCatalog} created by {@link HiveCatalogFactory}.
 */
public class HiveCatalogFactoryTest extends TestLogger {

	@Test
	public void test() {
		final String catalogName = "mycatalog";

		final HiveCatalog expectedCatalog = HiveTestUtils.createHiveCatalog(catalogName, null);

		final CatalogDescriptor catalogDescriptor = new HiveCatalogDescriptor();

		final Map<String, String> properties = catalogDescriptor.toProperties();

		final Catalog actualCatalog = TableFactoryService.find(CatalogFactory.class, properties)
			.createCatalog(catalogName, properties);

		checkEquals(expectedCatalog, (HiveCatalog) actualCatalog);
	}

	private static void checkEquals(HiveCatalog c1, HiveCatalog c2) {
		// Only assert a few selected properties for now
		assertEquals(c1.getName(), c2.getName());
		assertEquals(c1.getDefaultDatabase(), c2.getDefaultDatabase());
	}
}
